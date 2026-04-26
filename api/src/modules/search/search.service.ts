import { Injectable } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { SearchIndexerService } from './search-indexer.service';

type SearchBaseQueryDto = {
  channelIds?: string[];
  limit?: number;
  offset?: number;
  userId?: string;
  role?: string;
  docScope?: string;
  visibility?: string;
};

export interface SearchQueryDto extends SearchBaseQueryDto {
  keyword: string;
  fallbackToDb?: boolean;
}

export interface SearchHotQueryDto extends SearchBaseQueryDto {
  period?: string;
  fallbackToDb?: boolean;
}

export interface SearchTagsQueryDto extends SearchBaseQueryDto {}

export interface SearchByTagQueryDto extends SearchBaseQueryDto {
  tagId?: number;
  tagName?: string;
  level1Id?: number;
  fallbackToDb?: boolean;
}

export interface SearchByContentTagQueryDto extends SearchBaseQueryDto {
  tagId?: number;
  tagName?: string;
  scope?: string;
  fallbackToDb?: boolean;
}

@Injectable()
export class SearchService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly searchIndexerService: SearchIndexerService,
  ) {}

  async search(query: SearchQueryDto) {
    const { keyword, channelIds, userId, role, fallbackToDb = true } = query;
    const limit = this.sanitizeNumber(query.limit, 20, 1, 50);
    const offset = this.sanitizeNumber(query.offset, 0, 0, 100000);

    if (!keyword || keyword.trim().length < 2) {
      return { results: [], total: 0, hasMore: false, route: 'db' as const };
    }

    const trimmed = keyword.trim();
    const effectiveChannelIds = await this.resolveAllowedChannelIds({ channelIds, userId, role });
    const docTypes = this.resolveDocTypes(query.docScope);
    const visibilityValues = this.resolveVisibilityValues(query.visibility);

    try {
      const openSearchResult = await this.searchIndexerService.searchViaReadAlias({
        keyword: trimmed,
        channelIds: effectiveChannelIds,
        limit,
        offset,
        docTypes,
        visibilityValues,
      });

      if (openSearchResult.enabled) {
        const enrichedResults = await this.attachChannelTgChatId(openSearchResult.results);
        return {
          results: enrichedResults,
          total: openSearchResult.total,
          hasMore: offset + openSearchResult.results.length < openSearchResult.total,
          route: 'search-engine' as const,
        };
      }
    } catch {
      if (!fallbackToDb) {
        return { results: [], total: 0, hasMore: false, route: 'search-engine' as const };
      }
    }

    if (effectiveChannelIds.length === 0) {
      return { results: [], total: 0, hasMore: false, route: 'db' as const };
    }

    const titleContains = `%${trimmed}%`;
    const docTypeSql = this.buildTextInSql('sd.doc_type', docTypes);
    const visibilitySql = this.buildTextInSql('sd.visibility', visibilityValues);

    const whereSql = Prisma.sql`
      sd.is_active = true
      AND sd.is_deleted = false
      AND ${docTypeSql}
      AND ${visibilitySql}
      AND sd.channel_id = ANY(${effectiveChannelIds}::bigint[])
      AND (
        search_tsv @@ plainto_tsquery('simple', ${trimmed})
        OR sd.title ILIKE ${titleContains}
        OR EXISTS (SELECT 1 FROM unnest(sd.aliases) AS alias WHERE alias ILIKE ${titleContains})
        OR EXISTS (SELECT 1 FROM unnest(sd.actors) AS actor WHERE actor ILIKE ${titleContains})
        OR EXISTS (SELECT 1 FROM unnest(sd.keywords) AS keyword WHERE keyword ILIKE ${titleContains})
      )
    `;

    const rows = await this.prisma.$queryRaw<any[]>(Prisma.sql`
      SELECT
        sd.doc_id AS "docId",
        sd.doc_type AS "docType",
        sd.title,
        sd.original_title AS "originalTitle",
        sd.aliases,
        sd.actors,
        sd.year,
        sd.region,
        sd.description,
        sd.visibility,
        sd.telegram_message_link AS "telegramMessageLink",
        sd.telegram_message_id::text AS "telegramMessageId",
        sd.published_at AS "publishedAt",
        sd.quality_score::float AS "qualityScore",
        sd.popularity_score::float AS "popularityScore",
        sd.manual_weight::float AS "manualWeight",
        sd.collection_id::text AS "collectionId",
        sd.channel_id::text AS "channelId"
      FROM search_documents sd
      WHERE ${whereSql}
      ORDER BY
        CASE
          WHEN sd.title ILIKE ${trimmed} THEN 0
          WHEN sd.title ILIKE ${`${trimmed}%`} THEN 1
          WHEN sd.title ILIKE ${titleContains} THEN 2
          ELSE 3
        END ASC,
        (
          COALESCE(ts_rank(search_tsv, plainto_tsquery('simple', ${trimmed})), 0) * 0.65
          + COALESCE(sd.quality_score, 0) * 0.10
          + COALESCE(sd.popularity_score, 0) * 0.10
          + COALESCE(sd.manual_weight, 1.0) * 0.05
        ) DESC,
        sd.published_at DESC NULLS LAST
      LIMIT ${limit}
      OFFSET ${offset}
    `);

    const countRows = await this.prisma.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
      SELECT COUNT(*)::bigint AS count
      FROM search_documents sd
      WHERE ${whereSql}
    `);

    const total = Number(countRows[0]?.count ?? 0);
    const enrichedRows = await this.attachChannelTgChatId(rows);

    return {
      results: enrichedRows,
      total,
      hasMore: offset + rows.length < total,
      route: 'db' as const,
    };
  }

  async searchHot(query: SearchHotQueryDto) {
    const { channelIds, userId, role, fallbackToDb = true } = query;
    const limit = this.sanitizeNumber(query.limit, 20, 1, 50);
    const offset = this.sanitizeNumber(query.offset, 0, 0, 100000);
    const effectiveChannelIds = await this.resolveAllowedChannelIds({ channelIds, userId, role });
    const periodDays = this.parsePeriodDays(query.period);
    const docTypes = this.resolveDocTypes(query.docScope);
    const visibilityValues = this.resolveVisibilityValues(query.visibility);

    try {
      const openSearchResult = await this.searchIndexerService.searchHotViaReadAlias({
        channelIds: effectiveChannelIds,
        limit,
        offset,
        periodDays,
        docTypes,
        visibilityValues,
      });

      if (openSearchResult.enabled) {
        const enrichedResults = await this.attachChannelTgChatId(openSearchResult.results);
        return {
          results: enrichedResults,
          total: openSearchResult.total,
          hasMore: offset + openSearchResult.results.length < openSearchResult.total,
          route: 'search-engine' as const,
        };
      }
    } catch {
      if (!fallbackToDb) {
        return { results: [], total: 0, hasMore: false, route: 'search-engine' as const };
      }
    }

    if (effectiveChannelIds.length === 0) {
      return { results: [], total: 0, hasMore: false, route: 'db' as const };
    }

    const docTypeSql = this.buildTextInSql('sd.doc_type', docTypes);
    const visibilitySql = this.buildTextInSql('sd.visibility', visibilityValues);
    const periodSql = Prisma.sql`AND (sd.published_at IS NULL OR sd.published_at >= now() - (${periodDays} || ' days')::interval)`;

    const rows = await this.prisma.$queryRaw<any[]>(Prisma.sql`
      SELECT
        sd.doc_id AS "docId",
        sd.doc_type AS "docType",
        sd.title,
        sd.original_title AS "originalTitle",
        sd.aliases,
        sd.actors,
        sd.year,
        sd.region,
        sd.description,
        sd.visibility,
        sd.telegram_message_link AS "telegramMessageLink",
        sd.telegram_message_id::text AS "telegramMessageId",
        sd.published_at AS "publishedAt",
        sd.quality_score::float AS "qualityScore",
        sd.popularity_score::float AS "popularityScore",
        sd.manual_weight::float AS "manualWeight",
        sd.collection_id::text AS "collectionId",
        sd.channel_id::text AS "channelId"
      FROM search_documents sd
      WHERE
        sd.is_active = true
        AND sd.is_deleted = false
        AND ${docTypeSql}
        AND ${visibilitySql}
        AND sd.channel_id = ANY(${effectiveChannelIds}::bigint[])
        ${periodSql}
      ORDER BY
        COALESCE(sd.popularity_score, 0) DESC,
        sd.published_at DESC NULLS LAST,
        COALESCE(sd.quality_score, 0) DESC,
        sd.updated_at DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `);

    const countRows = await this.prisma.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
      SELECT COUNT(*)::bigint AS count
      FROM search_documents sd
      WHERE
        sd.is_active = true
        AND sd.is_deleted = false
        AND ${docTypeSql}
        AND ${visibilitySql}
        AND sd.channel_id = ANY(${effectiveChannelIds}::bigint[])
        ${periodSql}
    `);

    const total = Number(countRows[0]?.count ?? 0);
    const enrichedRows = await this.attachChannelTgChatId(rows);

    return {
      results: enrichedRows,
      total,
      hasMore: offset + rows.length < total,
      route: 'db' as const,
    };
  }

  async listTags(query: SearchTagsQueryDto) {
    const { channelIds, userId, role } = query;
    const limit = this.sanitizeNumber(query.limit, 30, 1, 100);
    const offset = this.sanitizeNumber(query.offset, 0, 0, 100000);
    const effectiveChannelIds = await this.resolveAllowedChannelIds({ channelIds, userId, role });
    const docTypes = this.resolveDocTypes(query.docScope);
    const visibilityValues = this.resolveVisibilityValues(query.visibility);

    if (effectiveChannelIds.length === 0) {
      return { tags: [], total: 0, hasMore: false, route: 'db' as const };
    }

    const docTypeSql = this.buildTextInSql('sd.doc_type', docTypes);
    const visibilitySql = this.buildTextInSql('sd.visibility', visibilityValues);

    const rows = await this.prisma.$queryRaw<Array<{ id: bigint; name: string; count: bigint }>>(Prisma.sql`
      SELECT
        l1.id,
        l1.name,
        COUNT(DISTINCT sd.id)::bigint AS count
      FROM category_level1 l1
      INNER JOIN category_level2 l2 ON l2.level1_id = l1.id
      INNER JOIN search_document_categories sdc ON sdc.level2_id = l2.id
      INNER JOIN search_documents sd ON sd.id = sdc.search_document_id
      WHERE
        l1.status = 'active'
        AND l2.status = 'active'
        AND sd.is_active = true
        AND sd.is_deleted = false
        AND ${docTypeSql}
        AND ${visibilitySql}
        AND sd.channel_id = ANY(${effectiveChannelIds}::bigint[])
      GROUP BY l1.id, l1.name
      ORDER BY l1.sort ASC, l1.name ASC
      LIMIT ${limit}
      OFFSET ${offset}
    `);

    const countRows = await this.prisma.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
      SELECT COUNT(*)::bigint AS count
      FROM (
        SELECT l1.id
        FROM category_level1 l1
        INNER JOIN category_level2 l2 ON l2.level1_id = l1.id
        INNER JOIN search_document_categories sdc ON sdc.level2_id = l2.id
        INNER JOIN search_documents sd ON sd.id = sdc.search_document_id
        WHERE
          l1.status = 'active'
          AND l2.status = 'active'
          AND sd.is_active = true
          AND sd.is_deleted = false
          AND ${docTypeSql}
          AND ${visibilitySql}
          AND sd.channel_id = ANY(${effectiveChannelIds}::bigint[])
        GROUP BY l1.id
      ) t
    `);

    const total = Number(countRows[0]?.count ?? 0);

    return {
      tags: rows.map((row) => ({
        id: row.id.toString(),
        name: row.name,
        level: 1,
        count: Number(row.count),
      })),
      total,
      hasMore: offset + rows.length < total,
      route: 'db' as const,
    };
  }

  async listLevel2Tags(query: SearchTagsQueryDto & { level1Id: number }) {
    const { channelIds, userId, role, level1Id } = query;
    const limit = this.sanitizeNumber(query.limit, 30, 1, 100);
    const offset = this.sanitizeNumber(query.offset, 0, 0, 100000);
    const effectiveChannelIds = await this.resolveAllowedChannelIds({ channelIds, userId, role });
    const docTypes = this.resolveDocTypes(query.docScope);
    const visibilityValues = this.resolveVisibilityValues(query.visibility);

    if (effectiveChannelIds.length === 0) {
      return { tags: [], total: 0, hasMore: false, route: 'db' as const };
    }

    const docTypeSql = this.buildTextInSql('sd.doc_type', docTypes);
    const visibilitySql = this.buildTextInSql('sd.visibility', visibilityValues);

    const rows = await this.prisma.$queryRaw<Array<{ id: bigint; name: string; level1Name: string; count: bigint }>>(Prisma.sql`
      SELECT
        l2.id,
        l2.name,
        l1.name AS "level1Name",
        COUNT(DISTINCT sd.id)::bigint AS count
      FROM category_level2 l2
      INNER JOIN category_level1 l1 ON l1.id = l2.level1_id
      INNER JOIN search_document_categories sdc ON sdc.level2_id = l2.id
      INNER JOIN search_documents sd ON sd.id = sdc.search_document_id
      WHERE
        l2.status = 'active'
        AND l1.status = 'active'
        AND l2.level1_id = ${BigInt(level1Id)}
        AND sd.is_active = true
        AND sd.is_deleted = false
        AND ${docTypeSql}
        AND ${visibilitySql}
        AND sd.channel_id = ANY(${effectiveChannelIds}::bigint[])
      GROUP BY l2.id, l2.name, l1.name, l2.sort
      ORDER BY l2.sort ASC, l2.name ASC
      LIMIT ${limit}
      OFFSET ${offset}
    `);

    const countRows = await this.prisma.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
      SELECT COUNT(*)::bigint AS count
      FROM (
        SELECT l2.id
        FROM category_level2 l2
        INNER JOIN search_document_categories sdc ON sdc.level2_id = l2.id
        INNER JOIN search_documents sd ON sd.id = sdc.search_document_id
        WHERE
          l2.status = 'active'
          AND l2.level1_id = ${BigInt(level1Id)}
          AND sd.is_active = true
          AND sd.is_deleted = false
          AND ${docTypeSql}
          AND ${visibilitySql}
          AND sd.channel_id = ANY(${effectiveChannelIds}::bigint[])
        GROUP BY l2.id
      ) t
    `);

    const total = Number(countRows[0]?.count ?? 0);

    return {
      tags: rows.map((row) => ({
        id: row.id.toString(),
        name: row.name,
        level: 2,
        level1Name: row.level1Name,
        count: Number(row.count),
      })),
      total,
      hasMore: offset + rows.length < total,
      route: 'db' as const,
    };
  }

  async listContentTags(query: SearchTagsQueryDto & { scope?: string }) {
    const { channelIds, userId, role, scope } = query;
    const limit = this.sanitizeNumber(query.limit, 30, 1, 100);
    const offset = this.sanitizeNumber(query.offset, 0, 0, 100000);
    const effectiveChannelIds = await this.resolveAllowedChannelIds({ channelIds, userId, role });
    const docTypes = this.resolveDocTypes(query.docScope);
    const visibilityValues = this.resolveVisibilityValues(query.visibility);

    if (effectiveChannelIds.length === 0) {
      return { tags: [], total: 0, hasMore: false, route: 'db' as const };
    }

    const docTypeSql = this.buildTextInSql('sd.doc_type', docTypes);
    const visibilitySql = this.buildTextInSql('sd.visibility', visibilityValues);

    const rows = await this.prisma.$queryRaw<Array<{ id: bigint; name: string; scope: string; count: bigint }>>(Prisma.sql`
      SELECT
        ct.id,
        ct.name,
        ct.scope,
        COUNT(DISTINCT sd.id)::bigint AS count
      FROM content_tags ct
      INNER JOIN search_document_tags sdt ON sdt.tag_id = ct.id
      INNER JOIN search_documents sd ON sd.id = sdt.search_document_id
      WHERE
        ct.status = 'active'
        AND (${scope ? Prisma.sql`ct.scope = ${scope}` : Prisma.sql`TRUE`})
        AND sd.is_active = true
        AND sd.is_deleted = false
        AND ${docTypeSql}
        AND ${visibilitySql}
        AND sd.channel_id = ANY(${effectiveChannelIds}::bigint[])
      GROUP BY ct.id, ct.name, ct.scope, ct.sort
      ORDER BY ct.sort ASC, ct.name ASC
      LIMIT ${limit}
      OFFSET ${offset}
    `);

    const countRows = await this.prisma.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
      SELECT COUNT(*)::bigint AS count
      FROM (
        SELECT ct.id
        FROM content_tags ct
        INNER JOIN search_document_tags sdt ON sdt.tag_id = ct.id
        INNER JOIN search_documents sd ON sd.id = sdt.search_document_id
        WHERE
          ct.status = 'active'
          AND (${scope ? Prisma.sql`ct.scope = ${scope}` : Prisma.sql`TRUE`})
          AND sd.is_active = true
          AND sd.is_deleted = false
          AND ${docTypeSql}
          AND ${visibilitySql}
          AND sd.channel_id = ANY(${effectiveChannelIds}::bigint[])
        GROUP BY ct.id
      ) t
    `);

    const total = Number(countRows[0]?.count ?? 0);

    return {
      tags: rows.map((row) => ({
        id: row.id.toString(),
        name: row.name,
        scope: row.scope,
        count: Number(row.count),
      })),
      total,
      hasMore: offset + rows.length < total,
      route: 'db' as const,
    };
  }

  async searchByTag(query: SearchByTagQueryDto) {
    const { tagId, tagName, level1Id, channelIds, userId, role, fallbackToDb = true } = query;
    const limit = this.sanitizeNumber(query.limit, 20, 1, 50);
    const offset = this.sanitizeNumber(query.offset, 0, 0, 100000);
    const effectiveChannelIds = await this.resolveAllowedChannelIds({ channelIds, userId, role });
    const docTypes = this.resolveDocTypes(query.docScope);
    const visibilityValues = this.resolveVisibilityValues(query.visibility);

    if (!tagId && !tagName) {
      return { results: [], total: 0, hasMore: false, route: 'db' as const };
    }

    try {
      const openSearchResult = await this.searchIndexerService.searchByTagViaReadAlias({
        channelIds: effectiveChannelIds,
        limit,
        offset,
        tagId,
        tagName: tagName?.trim(),
        level1Id,
        docTypes,
        visibilityValues,
      });

      if (openSearchResult.enabled) {
        const enrichedResults = await this.attachChannelTgChatId(openSearchResult.results);
        return {
          results: enrichedResults,
          total: openSearchResult.total,
          hasMore: offset + openSearchResult.results.length < openSearchResult.total,
          route: 'search-engine' as const,
        };
      }
    } catch {
      if (!fallbackToDb) {
        return { results: [], total: 0, hasMore: false, route: 'search-engine' as const };
      }
    }

    if (effectiveChannelIds.length === 0) {
      return { results: [], total: 0, hasMore: false, route: 'db' as const };
    }

    const docTypeSql = this.buildTextInSql('sd.doc_type', docTypes);
    const visibilitySql = this.buildTextInSql('sd.visibility', visibilityValues);
    const tagFilterSql = tagId
      ? Prisma.sql`AND l2.id = ${BigInt(tagId)}`
      : Prisma.sql`AND l2.name = ${String(tagName || '').trim()}`;
    const level1FilterSql = level1Id ? Prisma.sql`AND l2.level1_id = ${BigInt(level1Id)}` : Prisma.sql``;

    const rows = await this.prisma.$queryRaw<any[]>(Prisma.sql`
      SELECT
        sd.doc_id AS "docId",
        sd.doc_type AS "docType",
        sd.title,
        sd.original_title AS "originalTitle",
        sd.aliases,
        sd.actors,
        sd.year,
        sd.region,
        sd.description,
        sd.visibility,
        sd.telegram_message_link AS "telegramMessageLink",
        sd.telegram_message_id::text AS "telegramMessageId",
        sd.published_at AS "publishedAt",
        sd.quality_score::float AS "qualityScore",
        sd.popularity_score::float AS "popularityScore",
        sd.manual_weight::float AS "manualWeight",
        sd.collection_id::text AS "collectionId",
        sd.channel_id::text AS "channelId"
      FROM search_document_categories sdc
      INNER JOIN search_documents sd ON sd.id = sdc.search_document_id
      INNER JOIN category_level2 l2 ON l2.id = sdc.level2_id
      WHERE
        sd.is_active = true
        AND sd.is_deleted = false
        AND ${docTypeSql}
        AND ${visibilitySql}
        AND sd.channel_id = ANY(${effectiveChannelIds}::bigint[])
        ${tagFilterSql}
        ${level1FilterSql}
      ORDER BY
        COALESCE(sd.popularity_score, 0) DESC,
        sd.published_at DESC NULLS LAST,
        COALESCE(sd.quality_score, 0) DESC,
        sd.updated_at DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `);

    const countRows = await this.prisma.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
      SELECT COUNT(*)::bigint AS count
      FROM search_document_categories sdc
      INNER JOIN search_documents sd ON sd.id = sdc.search_document_id
      INNER JOIN category_level2 l2 ON l2.id = sdc.level2_id
      WHERE
        sd.is_active = true
        AND sd.is_deleted = false
        AND ${docTypeSql}
        AND ${visibilitySql}
        AND sd.channel_id = ANY(${effectiveChannelIds}::bigint[])
        ${tagFilterSql}
        ${level1FilterSql}
    `);

    const total = Number(countRows[0]?.count ?? 0);
    const enrichedRows = await this.attachChannelTgChatId(rows);

    return {
      results: enrichedRows,
      total,
      hasMore: offset + rows.length < total,
      route: 'db' as const,
    };
  }

  async searchByContentTag(query: SearchByContentTagQueryDto) {
    const { tagId, tagName, scope, channelIds, userId, role, fallbackToDb = true } = query;
    const limit = this.sanitizeNumber(query.limit, 20, 1, 50);
    const offset = this.sanitizeNumber(query.offset, 0, 0, 100000);
    const effectiveChannelIds = await this.resolveAllowedChannelIds({ channelIds, userId, role });
    const docTypes = this.resolveDocTypes(query.docScope);
    const visibilityValues = this.resolveVisibilityValues(query.visibility);

    if (!tagId && !tagName) {
      return { results: [], total: 0, hasMore: false, route: 'db' as const };
    }

    try {
      const openSearchResult = await this.searchIndexerService.searchByTagViaReadAlias({
        channelIds: effectiveChannelIds,
        limit,
        offset,
        tagId,
        tagName: tagName?.trim(),
        docTypes,
        visibilityValues,
      });

      if (openSearchResult.enabled) {
        const enrichedResults = await this.attachChannelTgChatId(openSearchResult.results);
        return {
          results: enrichedResults,
          total: openSearchResult.total,
          hasMore: offset + openSearchResult.results.length < openSearchResult.total,
          route: 'search-engine' as const,
        };
      }
    } catch {
      if (!fallbackToDb) {
        return { results: [], total: 0, hasMore: false, route: 'search-engine' as const };
      }
    }

    if (effectiveChannelIds.length === 0) {
      return { results: [], total: 0, hasMore: false, route: 'db' as const };
    }

    const docTypeSql = this.buildTextInSql('sd.doc_type', docTypes);
    const visibilitySql = this.buildTextInSql('sd.visibility', visibilityValues);
    const tagFilterSql = tagId
      ? Prisma.sql`AND ct.id = ${BigInt(tagId)}`
      : Prisma.sql`AND ct.name = ${String(tagName || '').trim()}`;
    const scopeFilterSql = scope ? Prisma.sql`AND ct.scope = ${scope}` : Prisma.sql``;

    const rows = await this.prisma.$queryRaw<any[]>(Prisma.sql`
      SELECT
        sd.doc_id AS "docId",
        sd.doc_type AS "docType",
        sd.title,
        sd.original_title AS "originalTitle",
        sd.aliases,
        sd.actors,
        sd.year,
        sd.region,
        sd.description,
        sd.visibility,
        sd.telegram_message_link AS "telegramMessageLink",
        sd.telegram_message_id::text AS "telegramMessageId",
        sd.published_at AS "publishedAt",
        sd.quality_score::float AS "qualityScore",
        sd.popularity_score::float AS "popularityScore",
        sd.manual_weight::float AS "manualWeight",
        sd.collection_id::text AS "collectionId",
        sd.channel_id::text AS "channelId"
      FROM search_document_tags sdt
      INNER JOIN search_documents sd ON sd.id = sdt.search_document_id
      INNER JOIN content_tags ct ON ct.id = sdt.tag_id
      WHERE
        sd.is_active = true
        AND sd.is_deleted = false
        AND ${docTypeSql}
        AND ${visibilitySql}
        AND sd.channel_id = ANY(${effectiveChannelIds}::bigint[])
        ${tagFilterSql}
        ${scopeFilterSql}
      ORDER BY
        COALESCE(sd.popularity_score, 0) DESC,
        sd.published_at DESC NULLS LAST,
        COALESCE(sd.quality_score, 0) DESC,
        sd.updated_at DESC
      LIMIT ${limit}
      OFFSET ${offset}
    `);

    const countRows = await this.prisma.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
      SELECT COUNT(*)::bigint AS count
      FROM search_document_tags sdt
      INNER JOIN search_documents sd ON sd.id = sdt.search_document_id
      INNER JOIN content_tags ct ON ct.id = sdt.tag_id
      WHERE
        sd.is_active = true
        AND sd.is_deleted = false
        AND ${docTypeSql}
        AND ${visibilitySql}
        AND sd.channel_id = ANY(${effectiveChannelIds}::bigint[])
        ${tagFilterSql}
        ${scopeFilterSql}
    `);

    const total = Number(countRows[0]?.count ?? 0);
    const enrichedRows = await this.attachChannelTgChatId(rows);

    return {
      results: enrichedRows,
      total,
      hasMore: offset + rows.length < total,
      route: 'db' as const,
    };
  }

  async getStats() {
    const [totalDocs, activeByType, outboxStats] = await Promise.all([
      this.prisma.searchDocument.count(),
      this.prisma.searchDocument.groupBy({
        by: ['docType'],
        where: { isActive: true, isDeleted: false },
        _count: { _all: true },
      }),
      this.prisma.searchIndexOutbox.groupBy({
        by: ['status'],
        _count: { _all: true },
      }),
    ]);

    return {
      totalDocuments: totalDocs,
      activeByType: activeByType.map((g) => ({
        docType: g.docType,
        count: g._count._all,
      })),
      outbox: outboxStats.map((g) => ({
        status: g.status,
        count: g._count._all,
      })),
    };
  }

  async processOutbox(limit?: number) {
    return this.searchIndexerService.processBatch(this.sanitizeNumber(limit, 50, 1, 200));
  }

  async initOpenSearch() {
    return this.searchIndexerService.initializeOpenSearchIndex();
  }

  async switchOpenSearchAliases(targetIndex: string) {
    return this.searchIndexerService.switchAliases(targetIndex);
  }

  private sanitizeNumber(value: number | undefined, fallback: number, min: number, max: number) {
    if (typeof value !== 'number' || Number.isNaN(value)) return fallback;
    return Math.min(max, Math.max(min, Math.trunc(value)));
  }

  private parsePeriodDays(period: string | undefined): number {
    const normalized = String(period || '').trim().toLowerCase();
    if (normalized === '3d') return 3;
    if (normalized === '30d') return 30;
    return 7;
  }

  private resolveDocTypes(scope: string | undefined): string[] {
    const normalized = String(scope || '')
      .trim()
      .toLowerCase();
    if (!normalized || normalized === 'all') {
      return ['asset', 'episode', 'collection'];
    }

    const mapped = normalized
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
      .flatMap((item) => {
        if (item === 'video' || item === 'asset') return ['asset'];
        if (item === 'episode') return ['episode'];
        if (item === 'collection') return ['collection'];
        return [];
      });

    return mapped.length > 0 ? Array.from(new Set(mapped)) : ['asset', 'episode', 'collection'];
  }

  private resolveVisibilityValues(raw: string | undefined): string[] {
    const normalized = String(raw || '')
      .trim()
      .toLowerCase();
    if (!normalized || normalized === 'all') {
      return ['public', 'adult_18'];
    }

    const values = normalized
      .split(',')
      .map((item) => item.trim())
      .filter((item) => ['public', 'adult_18', 'blocked'].includes(item));

    return values.length > 0 ? Array.from(new Set(values)) : ['public', 'adult_18'];
  }

  private buildTextInSql(columnName: string, values: string[]) {
    return Prisma.sql`${Prisma.raw(columnName)} IN (${Prisma.join(values.map((value) => Prisma.sql`${value}`))})`;
  }

  private async attachChannelTgChatId<T extends Record<string, unknown>>(rows: T[]) {
    if (!rows.length) return rows;

    const channelIds = Array.from(
      new Set(
        rows
          .map((row) => row.channelId ?? row.channel_id)
          .filter((id): id is string | number => id !== undefined && id !== null)
          .map((id) => String(id)),
      ),
    );

    if (!channelIds.length) return rows;

    const channels = await this.prisma.channel.findMany({
      where: { id: { in: channelIds.map((id) => BigInt(id)) } },
      select: { id: true, tgChatId: true },
    });

    const tgChatIdByChannelId = new Map(channels.map((c) => [c.id.toString(), c.tgChatId]));

    return rows.map((row) => {
      const channelId = row.channelId ?? row.channel_id;
      return {
        ...row,
        channelTgChatId: channelId ? tgChatIdByChannelId.get(String(channelId)) ?? null : null,
      };
    });
  }

  async resolveChannelIdsByTgChatIds(tgChatIds: string[]): Promise<string[]> {
    const normalized = tgChatIds.map((id) => id.trim()).filter(Boolean);
    if (normalized.length === 0) return [];

    const channels = await this.prisma.channel.findMany({
      where: {
        tgChatId: { in: normalized },
        status: 'active',
      },
      select: { id: true },
    });

    return channels.map((c) => c.id.toString());
  }

  private async resolveAllowedChannelIds(args: {
    channelIds?: string[];
    userId?: string;
    role?: string;
  }): Promise<bigint[]> {
    const requested = (args.channelIds || [])
      .map((id) => id.trim())
      .filter(Boolean)
      .map((id) => BigInt(id));

    if (args.role === 'admin') {
      if (requested.length > 0) return requested;
      const channels = await this.prisma.channel.findMany({
        where: { status: 'active' },
        select: { id: true },
      });
      return channels.map((c) => c.id);
    }

    const userOwned = await this.prisma.channel.findMany({
      where: {
        status: 'active',
        createdBy: args.userId ? BigInt(args.userId) : undefined,
      },
      select: { id: true },
    });

    const userOwnedSet = new Set(userOwned.map((c) => c.id.toString()));
    if (requested.length === 0) {
      return userOwned.map((c) => c.id);
    }

    return requested.filter((id) => userOwnedSet.has(id.toString()));
  }
}

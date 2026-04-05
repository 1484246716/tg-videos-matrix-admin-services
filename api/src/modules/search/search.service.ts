import { Injectable } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { SearchIndexerService } from './search-indexer.service';

export interface SearchQueryDto {
  keyword: string;
  channelIds?: string[];
  limit?: number;
  offset?: number;
  userId?: string;
  role?: string;
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

    try {
      const openSearchResult = await this.searchIndexerService.searchViaReadAlias({
        keyword: trimmed,
        channelIds: effectiveChannelIds,
        limit,
        offset,
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

    const whereSql = Prisma.sql`
      is_active = true
      AND is_deleted = false
      AND doc_type <> 'collection'
      AND channel_id = ANY(${effectiveChannelIds}::bigint[])
      AND (
        search_tsv @@ plainto_tsquery('simple', ${trimmed})
        OR title ILIKE ${titleContains}
        OR EXISTS (SELECT 1 FROM unnest(aliases) AS alias WHERE alias ILIKE ${titleContains})
        OR EXISTS (SELECT 1 FROM unnest(actors) AS actor WHERE actor ILIKE ${titleContains})
      )
    `;

    const rows = await this.prisma.$queryRaw<any[]>(Prisma.sql`
      SELECT
        doc_id AS "docId",
        doc_type AS "docType",
        title,
        original_title AS "originalTitle",
        aliases,
        actors,
        year,
        region,
        description,
        telegram_message_link AS "telegramMessageLink",
        telegram_message_id::text AS "telegramMessageId",
        published_at AS "publishedAt",
        quality_score::float AS "qualityScore",
        popularity_score::float AS "popularityScore",
        manual_weight::float AS "manualWeight",
        collection_id::text AS "collectionId",
        channel_id::text AS "channelId"
      FROM search_documents
      WHERE ${whereSql}
      ORDER BY
        CASE
          WHEN title ILIKE ${trimmed} THEN 0
          WHEN title ILIKE ${`${trimmed}%`} THEN 1
          WHEN title ILIKE ${titleContains} THEN 2
          ELSE 3
        END ASC,
        (
          COALESCE(ts_rank(search_tsv, plainto_tsquery('simple', ${trimmed})), 0) * 0.65
          + COALESCE(quality_score, 0) * 0.10
          + COALESCE(popularity_score, 0) * 0.10
          + COALESCE(manual_weight, 1.0) * 0.05
        ) DESC,
        published_at DESC NULLS LAST
      LIMIT ${limit}
      OFFSET ${offset}
    `);

    const countRows = await this.prisma.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
      SELECT COUNT(*)::bigint AS count
      FROM search_documents
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

  private async attachChannelTgChatId<T extends { channelId?: string | number }>(rows: T[]) {
    if (!rows.length) return rows;

    const channelIds = Array.from(
      new Set(
        rows
          .map((row) => row.channelId)
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

    return rows.map((row) => ({
      ...row,
      channelTgChatId: row.channelId ? tgChatIdByChannelId.get(String(row.channelId)) ?? null : null,
    }));
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

    // admin 可查任意频道（若传 channelIds 则按传入过滤）
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

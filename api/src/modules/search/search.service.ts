import { Injectable } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

export interface SearchQueryDto {
  keyword: string;
  channelIds?: string[];
  limit?: number;
  offset?: number;
}

export interface SearchResultItem {
  docId: string;
  docType: string;
  title: string;
  originalTitle: string | null;
  actors: string[];
  year: number | null;
  region: string | null;
  description: string | null;
  telegramMessageLink: string | null;
  telegramMessageId: string | null;
  publishedAt: Date | null;
  qualityScore: number;
  popularityScore: number;
  collectionId: string | null;
  channelId: string;
}

@Injectable()
export class SearchService {
  constructor(private readonly prisma: PrismaService) {}

  async search(query: SearchQueryDto) {
    const { keyword, channelIds, limit = 20, offset = 0 } = query;

    if (!keyword || keyword.trim().length < 2) {
      return { results: [], total: 0, hasMore: false };
    }

    const trimmed = keyword.trim();
    const effectiveLimit = Math.min(limit, 50);

    // 构建频道过滤
    const channelFilter = channelIds?.length
      ? channelIds.map(id => BigInt(id))
      : null;

    // 如果没有指定频道，搜索所有活跃频道
    const activeChannelIds = channelFilter ?? (await this.getActiveChannelIds());

    if (activeChannelIds.length === 0) {
      return { results: [], total: 0, hasMore: false };
    }

    const results = await this.prisma.$queryRaw<any[]>`
      SELECT
        doc_id AS "docId",
        doc_type AS "docType",
        title,
        original_title AS "originalTitle",
        actors,
        year,
        region,
        description,
        telegram_message_link AS "telegramMessageLink",
        telegram_message_id::text AS "telegramMessageId",
        published_at AS "publishedAt",
        quality_score::float AS "qualityScore",
        popularity_score::float AS "popularityScore",
        collection_id::text AS "collectionId",
        channel_id::text AS "channelId"
      FROM search_documents
      WHERE is_active = true
        AND is_deleted = false
        AND channel_id = ANY(${activeChannelIds}::bigint[])
        AND (
          search_tsv @@ plainto_tsquery('simple', ${trimmed})
          OR title ILIKE ${'%' + trimmed + '%'}
          OR EXISTS (
            SELECT 1 FROM unnest(aliases) AS alias
            WHERE alias ILIKE ${'%' + trimmed + '%'}
          )
          OR EXISTS (
            SELECT 1 FROM unnest(actors) AS actor
            WHERE actor ILIKE ${'%' + trimmed + '%'}
          )
        )
      ORDER BY
        CASE WHEN title ILIKE ${trimmed} THEN 0
             WHEN title ILIKE ${trimmed + '%'} THEN 1
             WHEN title ILIKE ${'%' + trimmed + '%'} THEN 2
             ELSE 3
        END ASC,
        (
          COALESCE(ts_rank(search_tsv, plainto_tsquery('simple', ${trimmed})), 0) * 0.65
          + COALESCE(quality_score, 0) * 0.10
          + COALESCE(popularity_score, 0) * 0.10
          + COALESCE(manual_weight, 1.0) * 0.05
        ) DESC,
        published_at DESC NULLS LAST
      LIMIT ${effectiveLimit}
      OFFSET ${offset}
    `;

    const countResult = await this.prisma.$queryRaw<[{ count: bigint }]>`
      SELECT COUNT(*) as count
      FROM search_documents
      WHERE is_active = true
        AND is_deleted = false
        AND channel_id = ANY(${activeChannelIds}::bigint[])
        AND (
          search_tsv @@ plainto_tsquery('simple', ${trimmed})
          OR title ILIKE ${'%' + trimmed + '%'}
          OR EXISTS (
            SELECT 1 FROM unnest(aliases) AS alias
            WHERE alias ILIKE ${'%' + trimmed + '%'}
          )
          OR EXISTS (
            SELECT 1 FROM unnest(actors) AS actor
            WHERE actor ILIKE ${'%' + trimmed + '%'}
          )
        )
    `;

    const total = Number(countResult[0]?.count ?? 0);

    return {
      results,
      total,
      hasMore: offset + results.length < total,
    };
  }

  /** 获取搜索索引统计信息 */
  async getStats() {
    const [totalDocs, activeByType, outboxStats] = await Promise.all([
      this.prisma.searchDocument.count(),
      this.prisma.searchDocument.groupBy({
        by: ['docType'],
        where: { isActive: true, isDeleted: false },
        _count: true,
      }),
      this.prisma.searchIndexOutbox.groupBy({
        by: ['status'],
        _count: true,
      }),
    ]);

    return {
      totalDocuments: totalDocs,
      activeByType: activeByType.map(g => ({
        docType: g.docType,
        count: g._count,
      })),
      outbox: outboxStats.map(g => ({
        status: g.status,
        count: g._count,
      })),
    };
  }

  private async getActiveChannelIds(): Promise<bigint[]> {
    const channels = await this.prisma.channel.findMany({
      where: { status: 'active' },
      select: { id: true },
    });
    return channels.map(c => c.id);
  }
}

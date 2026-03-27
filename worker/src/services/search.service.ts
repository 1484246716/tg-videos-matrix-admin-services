/**
 * SearchService
 * ─────────────
 * Postgres 兜底搜索 API（阶段A 唯一搜索通道）
 * 后续阶段B 接入 OpenSearch 后，此为降级查询路径。
 *
 * 搜索策略（§7）：
 * score = 文本相关性 * 0.65 + 新鲜度 * 0.10 + 质量分 * 0.10 + 热度 * 0.10 + 人工权重 * 0.05
 * 优先级：title > aliases > actors > keywords > description/search_text
 *
 * 强制过滤：
 * - is_active = true
 * - is_deleted = false
 * - channel_id IN 用户可访问频道
 */
import { prisma } from '../infra/prisma';
import { logger } from '../logger';
import { Prisma } from '@prisma/client';

export interface SearchQuery {
  keyword: string;
  channelIds: bigint[];
  limit?: number;
  offset?: number;
}

export interface SearchResult {
  docId: string;
  docType: string;
  title: string;
  originalTitle: string | null;
  actors: string[];
  year: number | null;
  region: string | null;
  description: string | null;
  telegramMessageLink: string | null;
  telegramMessageId: bigint | null;
  publishedAt: Date | null;
  qualityScore: number;
  popularityScore: number;
  collectionId: bigint | null;
  channelId: bigint;
}

export interface SearchResponse {
  results: SearchResult[];
  total: number;
  hasMore: boolean;
}

/**
 * 执行搜索（Postgres 兜底）
 * 使用 search_tsv 全文索引 + 标题 ILIKE 模糊搜索组合
 */
export async function search(query: SearchQuery): Promise<SearchResponse> {
  const { keyword, channelIds, limit = 20, offset = 0 } = query;

  if (!keyword || keyword.trim().length < 2) {
    return { results: [], total: 0, hasMore: false };
  }

  if (channelIds.length === 0) {
    return { results: [], total: 0, hasMore: false };
  }

  const trimmed = keyword.trim();
  const effectiveLimit = Math.min(limit, 50); // Telegram inline query 最大 50 条

  try {
    // 使用原始 SQL 以便利用 ts_rank + search_tsv
    const results = await prisma.$queryRaw<SearchResult[]>`
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
        telegram_message_id AS "telegramMessageId",
        published_at AS "publishedAt",
        quality_score AS "qualityScore",
        popularity_score AS "popularityScore",
        collection_id AS "collectionId",
        channel_id AS "channelId"
      FROM search_documents
      WHERE is_active = true
        AND is_deleted = false
        AND channel_id = ANY(${channelIds}::bigint[])
        AND (
          -- 全文索引匹配
          search_tsv @@ plainto_tsquery('simple', ${trimmed})
          -- 标题模糊匹配（兜底）
          OR title ILIKE ${'%' + trimmed + '%'}
          -- 别名数组匹配
          OR EXISTS (
            SELECT 1 FROM unnest(aliases) AS alias
            WHERE alias ILIKE ${'%' + trimmed + '%'}
          )
          -- 演员数组匹配
          OR EXISTS (
            SELECT 1 FROM unnest(actors) AS actor
            WHERE actor ILIKE ${'%' + trimmed + '%'}
          )
        )
      ORDER BY
        -- 标题精确匹配最高权重
        CASE WHEN title ILIKE ${trimmed} THEN 0
             WHEN title ILIKE ${trimmed + '%'} THEN 1
             WHEN title ILIKE ${'%' + trimmed + '%'} THEN 2
             ELSE 3
        END ASC,
        -- 综合评分
        (
          COALESCE(ts_rank(search_tsv, plainto_tsquery('simple', ${trimmed})), 0) * 0.65
          + COALESCE(quality_score, 0) * 0.10
          + COALESCE(popularity_score, 0) * 0.10
          + COALESCE(manual_weight, 1.0) * 0.05
        ) DESC,
        -- 新鲜度（最新优先）
        published_at DESC NULLS LAST
      LIMIT ${effectiveLimit}
      OFFSET ${offset}
    `;

    // 获取总数（简易版，不做精确 COUNT 避免全表扫描）
    const countResult = await prisma.$queryRaw<[{ count: bigint }]>`
      SELECT COUNT(*) as count
      FROM search_documents
      WHERE is_active = true
        AND is_deleted = false
        AND channel_id = ANY(${channelIds}::bigint[])
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

    logger.info('[search] 搜索完成', {
      keyword: trimmed,
      channelIds: channelIds.map(String),
      resultCount: results.length,
      total,
      offset,
    });

    return {
      results,
      total,
      hasMore: offset + results.length < total,
    };
  } catch (error) {
    logger.error('[search] 搜索异常', { keyword: trimmed, error });
    throw error;
  }
}

// ──────────────────────────── 频道过滤辅助 ────────────────────────────

export interface SearchContext {
  source: 'channel_inline' | 'bot_private' | 'admin';
  chatId?: bigint;
  botId?: bigint;
  allowedChannelIds?: bigint[];
}

/**
 * 根据搜索来源确定可访问的 channel_ids（§20）
 */
export async function buildChannelFilter(context: SearchContext): Promise<bigint[]> {
  if (context.source === 'channel_inline' && context.chatId) {
    const channel = await prisma.channel.findUnique({
      where: { tgChatId: String(context.chatId) },
      select: { id: true },
    });
    return channel ? [channel.id] : [];
  }

  if (context.source === 'bot_private' && context.botId) {
    const channels = await prisma.channel.findMany({
      where: { defaultBotId: context.botId, status: 'active' },
      select: { id: true },
    });
    return channels.map(c => c.id);
  }

  // 管理后台：按用户权限过滤
  return context.allowedChannelIds ?? [];
}

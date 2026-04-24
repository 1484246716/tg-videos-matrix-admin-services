/**
 * ???????????????????????????????????????????
 * ?????search-index.service ??? upsert search_documents ??????????????
 */

/**
 * SearchDocumentBuilder
 * ─────────────────────
 * 将业务多表数据（media_assets / collection_episodes / collections）
 * 转换为统一的 search_documents 结构。
 *
 * 设计原则：
 * - 业务字段变化只改 Builder，不改搜索 API。
 * - 输出必须补齐：doc_id / doc_type / title / search_text / schema_version / source_updated_at。
 */
import { normalizeTitle, buildSearchText, extractActorsFromTitle } from './search-text-normalizer';

const SCHEMA_VERSION = 1;

// ──────────────────────────── 类型定义 ────────────────────────────

/** Prisma Collection (with channel) */
export interface CollectionInput {
  id: bigint;
  channelId: bigint;
  name: string;
  description: string | null;
  status: string;
  updatedAt: Date;
  channel: { id: bigint; tgChatId: string };
}

/** Prisma CollectionEpisode (with collection + mediaAsset) */
export interface EpisodeInput {
  id: bigint;
  collectionId: bigint;
  episodeNo: number;
  episodeTitle: string | null;
  fileNameSnapshot: string;
  telegramMessageId: bigint | null;
  telegramMessageLink: string | null;
  publishedAt: Date | null;
  updatedAt: Date;
  collection: {
    id: bigint;
    channelId: bigint;
    name: string;
    description: string | null;
    channel: { id: bigint; tgChatId: string };
  };
  mediaAsset: {
    id: bigint;
    originalName: string;
    aiGeneratedCaption: string | null;
    sourceMeta: Record<string, unknown> | null;
    updatedAt: Date;
  };
}

/** Prisma MediaAsset (standalone, non-collection) + its successful dispatch task */
export interface MediaAssetInput {
  id: bigint;
  channelId: bigint;
  originalName: string;
  aiGeneratedCaption: string | null;
  sourceMeta: Record<string, unknown> | null;
  updatedAt: Date;
  channel: { id: bigint; tgChatId: string };
  dispatchTasks: Array<{
    telegramMessageId: bigint | null;
    telegramMessageLink: string | null;
    finishedAt: Date | null;
    channelId: bigint;
  }>;
}

/** Unified search document output (matches Prisma SearchDocument create input) */
export interface SearchDocOutput {
  docId: string;
  docType: string;
  schemaVersion: number;
  channelId: bigint;
  collectionId: bigint | null;
  mediaAssetId: bigint | null;
  episodeId: bigint | null;
  title: string;
  originalTitle: string | null;
  aliases: string[];
  actors: string[];
  directors: string[];
  genres: string[];
  keywords: string[];
  year: number | null;
  region: string | null;
  language: string | null;
  description: string | null;
  searchText: string;
  telegramMessageLink: string | null;
  telegramMessageId: bigint | null;
  publishedAt: Date | null;
  qualityScore: number;
  popularityScore: number;
  manualWeight: number;
  visibility: string;
  isActive: boolean;
  isDeleted: boolean;
  ext: Record<string, unknown> | null;
  sourceUpdatedAt: Date;
  indexedAt: Date;
}

// ──────────────────────────── Builder ────────────────────────────

export class SearchDocumentBuilder {
  /**
   * 从合集构建搜索文档
   * doc_id: collection:{id}
   */
  static fromCollection(c: CollectionInput): SearchDocOutput {
    const title = normalizeTitle(c.name);
    const actors = extractActorsFromTitle(c.name);
    const now = new Date();

    return {
      docId: `collection:${c.id}`,
      docType: 'collection',
      schemaVersion: SCHEMA_VERSION,
      channelId: c.channelId,
      collectionId: c.id,
      mediaAssetId: null,
      episodeId: null,
      title,
      originalTitle: c.name !== title ? c.name : null,
      aliases: [],
      actors,
      directors: [],
      genres: [],
      keywords: [],
      year: extractYear(c.name),
      region: null,
      language: null,
      description: c.description,
      searchText: buildSearchText({
        title: c.name,
        actors,
        description: c.description,
      }),
      telegramMessageLink: null,
      telegramMessageId: null,
      publishedAt: null,
      qualityScore: calculateQualityScore({ title, actors, description: c.description }),
      popularityScore: 0,
      manualWeight: 1.0,
      visibility: 'public',
      isActive: c.status === 'active',
      isDeleted: false,
      ext: null,
      sourceUpdatedAt: c.updatedAt,
      indexedAt: now,
    };
  }

  /**
   * 从分集构建搜索文档
   * doc_id: episode:{id}
   */
  static fromEpisode(ep: EpisodeInput): SearchDocOutput {
    // 使用分集标题或合集名+集号
    const rawTitle = ep.episodeTitle || ep.fileNameSnapshot || `${ep.collection.name} 第${ep.episodeNo}集`;
    const title = normalizeTitle(rawTitle);
    const actorsFromFile = extractActorsFromTitle(ep.fileNameSnapshot);
    const actorsFromCaption = ep.mediaAsset.aiGeneratedCaption
      ? extractActorsFromCaption(ep.mediaAsset.aiGeneratedCaption)
      : [];
    const actors = dedup([...actorsFromFile, ...actorsFromCaption]);
    const now = new Date();

    // 从 sourceMeta 可能提取额外信息
    const meta = ep.mediaAsset.sourceMeta ?? {};
    const keywords: string[] = [];
    if (ep.collection.name) keywords.push(normalizeTitle(ep.collection.name));

    return {
      docId: `episode:${ep.id}`,
      docType: 'episode',
      schemaVersion: SCHEMA_VERSION,
      channelId: ep.collection.channelId,
      collectionId: ep.collectionId,
      mediaAssetId: ep.mediaAsset.id,
      episodeId: ep.id,
      title,
      originalTitle: rawTitle !== title ? rawTitle : null,
      aliases: [],
      actors,
      directors: [],
      genres: [],
      keywords,
      year: extractYear(rawTitle) || extractYear(ep.collection.name),
      region: null,
      language: null,
      description: ep.mediaAsset.aiGeneratedCaption || ep.collection.description || null,
      searchText: buildSearchText({
        title: rawTitle,
        aliases: [ep.collection.name],
        actors,
        keywords,
        description: ep.mediaAsset.aiGeneratedCaption || ep.collection.description || null,
      }),
      telegramMessageLink: ep.telegramMessageLink || null,
      telegramMessageId: ep.telegramMessageId || null,
      publishedAt: ep.publishedAt || null,
      qualityScore: calculateQualityScore({
        title,
        actors,
        description: ep.mediaAsset.aiGeneratedCaption,
      }),
      popularityScore: 0,
      manualWeight: 1.0,
      visibility: 'public',
      isActive: true,
      isDeleted: false,
      ext: Object.keys(meta).length > 0 ? meta : null,
      sourceUpdatedAt: laterDate(ep.updatedAt, ep.mediaAsset.updatedAt),
      indexedAt: now,
    };
  }

  /**
   * 从独立媒体资源构建搜索文档
   * doc_id: asset:{id}
   */
  static fromMediaAsset(
    asset: MediaAssetInput,
    dispatch?: MediaAssetInput['dispatchTasks'][0],
  ): SearchDocOutput {
    const rawTitle = asset.originalName;
    const title = normalizeTitle(rawTitle);
    const actors = extractActorsFromTitle(rawTitle);
    const now = new Date();

    return {
      docId: `asset:${asset.id}`,
      docType: 'asset',
      schemaVersion: SCHEMA_VERSION,
      channelId: dispatch?.channelId ?? asset.channelId,
      collectionId: null,
      mediaAssetId: asset.id,
      episodeId: null,
      title,
      originalTitle: rawTitle !== title ? rawTitle : null,
      aliases: [],
      actors,
      directors: [],
      genres: [],
      keywords: [],
      year: extractYear(rawTitle),
      region: null,
      language: null,
      description: asset.aiGeneratedCaption,
      searchText: buildSearchText({
        title: rawTitle,
        actors,
        description: asset.aiGeneratedCaption,
      }),
      telegramMessageLink: dispatch?.telegramMessageLink || null,
      telegramMessageId: dispatch?.telegramMessageId || null,
      publishedAt: dispatch?.finishedAt || null,
      qualityScore: calculateQualityScore({
        title,
        actors,
        description: asset.aiGeneratedCaption,
      }),
      popularityScore: 0,
      manualWeight: 1.0,
      visibility: 'public',
      isActive: true,
      isDeleted: false,
      ext: asset.sourceMeta && Object.keys(asset.sourceMeta).length > 0 ? asset.sourceMeta : null,
      sourceUpdatedAt: asset.updatedAt,
      indexedAt: now,
    };
  }
}

// ──────────────────────────── 辅助函数 ────────────────────────────

function extractYear(text: string): number | null {
  const match = text.match(/(19|20)\d{2}/);
  return match ? parseInt(match[0], 10) : null;
}

// ?? calculate Quality Score ?????????????????????
function calculateQualityScore(doc: {
  title?: string;
  actors?: string[];
  description?: string | null;
}): number {
  let score = 0.4; // 基础分（有标题即 0.4）
  if (doc.actors && doc.actors.length > 0) score += 0.3;
  if (doc.description && doc.description.length > 20) score += 0.3;
  return Math.min(score, 1.0);
}

// ?? later Date ?????????????????????
function laterDate(a: Date, b: Date): Date {
  return a.getTime() >= b.getTime() ? a : b;
}

// ?? dedup ?????????????????????
function dedup(arr: string[]): string[] {
  return [...new Set(arr)];
}

/**
 * 尝试从AI生成的字幕/介绍中提取中文人名（简单模式）
 */
function extractActorsFromCaption(caption: string): string[] {
  // 匹配 "主演：xxx、yyy" 或 "演员：xxx, yyy" 等
  const patterns = [
    /(?:主演|演员|表演|出演)[：:]\s*(.+?)(?:\n|$)/,
    /(?:由|featuring)\s+(.+?)(?:主演|出演|表演)/,
  ];

  for (const pattern of patterns) {
    const match = caption.match(pattern);
    if (match) {
      return match[1]
        .split(/[、,，\s]+/)
        .map(s => s.trim())
        .filter(s => s.length >= 2 && s.length <= 6 && /^[\u4e00-\u9fa5]+$/.test(s));
    }
  }

  return [];
}

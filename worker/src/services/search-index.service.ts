/**
 * SearchIndexService
 * ──────────────────
 * 处理 q_search_index 队列任务：
 * 1. 根据 sourceType + sourceId 从业务表读取数据
 * 2. 通过 SearchDocumentBuilder 构建文档
 * 3. 幂等 upsert 到 search_documents
 * 4. 写 outbox 记录（用于后续同步 OpenSearch）
 *
 * 幂等规则（§13.7）：
 * - incoming.source_updated_at >= current.source_updated_at 才更新
 * - doc_id 唯一约束不可放松
 */
import { prisma } from '../infra/prisma';
import { logger, logError } from '../logger';
import {
  SearchDocumentBuilder,
  type SearchDocOutput,
  type CollectionInput,
  type EpisodeInput,
  type MediaAssetInput,
} from './search-document-builder';

export type SearchIndexJobData = {
  sourceType: 'collection' | 'episode' | 'media_asset' | 'dispatch_task' | 'collection_episode';
  sourceId: string;          // BigInt.toString()
  mediaAssetId?: string;     // BigInt.toString()
  channelId?: string;        // BigInt.toString()
  action?: 'upsert' | 'delete';
};

/**
 * 处理单个搜索索引任务
 */
export async function handleSearchIndexJob(data: SearchIndexJobData): Promise<void> {
  const { sourceType, sourceId, action = 'upsert' } = data;

  logger.info('[q_search_index] 处理任务', { sourceType, sourceId, action });

  if (action === 'delete') {
    await handleDelete(sourceType, sourceId);
    return;
  }

  let doc: SearchDocOutput | null = null;

  try {
    switch (sourceType) {
      case 'collection':
        doc = await buildFromCollection(BigInt(sourceId));
        break;
      case 'episode':
      case 'collection_episode':
        doc = await buildFromEpisode(BigInt(sourceId));
        break;
      case 'media_asset':
      case 'dispatch_task':
        doc = await buildFromMediaAsset(
          BigInt(data.mediaAssetId || sourceId),
        );
        break;
      default:
        logger.warn('[q_search_index] 未知 sourceType', { sourceType });
        return;
    }
  } catch (err) {
    logError('[q_search_index] 文档构建失败', { sourceType, sourceId, error: err });
    throw err;
  }

  if (!doc) {
    logger.info('[q_search_index] 无法构建文档（数据不存在或无效）', { sourceType, sourceId });
    return;
  }

  await upsertSearchDocument(doc);
}

// ──────────────────────────── 构建逻辑 ────────────────────────────

async function buildFromCollection(collectionId: bigint): Promise<SearchDocOutput | null> {
  const c = await prisma.collection.findUnique({
    where: { id: collectionId },
    include: { channel: { select: { id: true, tgChatId: true } } },
  });
  if (!c) return null;

  return SearchDocumentBuilder.fromCollection(c as CollectionInput);
}

async function buildFromEpisode(episodeId: bigint): Promise<SearchDocOutput | null> {
  const ep = await prisma.collectionEpisode.findUnique({
    where: { id: episodeId },
    include: {
      collection: {
        include: { channel: { select: { id: true, tgChatId: true } } },
      },
      mediaAsset: {
        select: {
          id: true,
          originalName: true,
          aiGeneratedCaption: true,
          sourceMeta: true,
          updatedAt: true,
        },
      },
    },
  });
  if (!ep) return null;

  return SearchDocumentBuilder.fromEpisode(ep as unknown as EpisodeInput);
}

async function buildFromMediaAsset(mediaAssetId: bigint): Promise<SearchDocOutput | null> {
  const asset = await prisma.mediaAsset.findUnique({
    where: { id: mediaAssetId },
    include: {
      channel: { select: { id: true, tgChatId: true } },
      collectionEpisode: { select: { id: true } },
      dispatchTasks: {
        where: { status: 'success', telegramMessageId: { not: null } },
        take: 1,
        orderBy: { finishedAt: 'desc' },
        select: {
          telegramMessageId: true,
          telegramMessageLink: true,
          finishedAt: true,
          channelId: true,
        },
      },
    },
  });

  if (!asset) return null;

  // 如果是合集分集，走 episode 构建路径
  if (asset.collectionEpisode) {
    return buildFromEpisode(asset.collectionEpisode.id);
  }

  // 跳过没有成功发送记录的
  if (asset.dispatchTasks.length === 0) return null;

  return SearchDocumentBuilder.fromMediaAsset(
    asset as unknown as MediaAssetInput,
    asset.dispatchTasks[0],
  );
}

// ──────────────────────────── 幂等 Upsert ────────────────────────────

async function upsertSearchDocument(doc: SearchDocOutput): Promise<void> {
  // 检查是否存在更新的版本（§13.7 幂等与并发冲突规则）
  const existing = await prisma.searchDocument.findUnique({
    where: { docId: doc.docId },
    select: { sourceUpdatedAt: true },
  });

  if (existing && existing.sourceUpdatedAt.getTime() > doc.sourceUpdatedAt.getTime()) {
    logger.info('[q_search_index] 跳过旧版本文档', {
      docId: doc.docId,
      existingUpdatedAt: existing.sourceUpdatedAt.toISOString(),
      incomingUpdatedAt: doc.sourceUpdatedAt.toISOString(),
    });
    return;
  }

  const data = {
    docType: doc.docType,
    schemaVersion: doc.schemaVersion,
    channelId: doc.channelId,
    collectionId: doc.collectionId,
    mediaAssetId: doc.mediaAssetId,
    episodeId: doc.episodeId,
    title: doc.title,
    originalTitle: doc.originalTitle,
    aliases: doc.aliases,
    actors: doc.actors,
    directors: doc.directors,
    genres: doc.genres,
    keywords: doc.keywords,
    year: doc.year,
    region: doc.region,
    language: doc.language,
    description: doc.description,
    searchText: doc.searchText,
    telegramMessageLink: doc.telegramMessageLink,
    telegramMessageId: doc.telegramMessageId,
    publishedAt: doc.publishedAt,
    qualityScore: doc.qualityScore,
    popularityScore: doc.popularityScore,
    manualWeight: doc.manualWeight,
    visibility: doc.visibility,
    isActive: doc.isActive,
    isDeleted: doc.isDeleted,
    ext: doc.ext ?? undefined,
    sourceUpdatedAt: doc.sourceUpdatedAt,
    indexedAt: doc.indexedAt,
  };

  await prisma.searchDocument.upsert({
    where: { docId: doc.docId },
    create: { docId: doc.docId, ...data },
    update: data,
  });

  // 写 outbox（用于后续同步 OpenSearch）
  await prisma.searchIndexOutbox.create({
    data: {
      docId: doc.docId,
      op: 'upsert',
      payload: data as any,
    },
  });

  logger.info('[q_search_index] 文档已索引', {
    docId: doc.docId,
    docType: doc.docType,
    title: doc.title,
  });
}

// ──────────────────────────── 删除/下线 ────────────────────────────

async function handleDelete(sourceType: string, sourceId: string): Promise<void> {
  let docId: string;

  switch (sourceType) {
    case 'collection':
      docId = `collection:${sourceId}`;
      break;
    case 'episode':
    case 'collection_episode':
      docId = `episode:${sourceId}`;
      break;
    case 'media_asset':
      docId = `asset:${sourceId}`;
      break;
    default:
      logger.warn('[q_search_index] 删除：未知 sourceType', { sourceType });
      return;
  }

  const existing = await prisma.searchDocument.findUnique({
    where: { docId },
    select: { id: true },
  });

  if (!existing) {
    logger.info('[q_search_index] 删除：文档不存在', { docId });
    return;
  }

  await prisma.searchDocument.update({
    where: { docId },
    data: { isDeleted: true, isActive: false },
  });

  await prisma.searchIndexOutbox.create({
    data: {
      docId,
      op: 'delete',
    },
  });

  logger.info('[q_search_index] 文档已标记删除', { docId });
}

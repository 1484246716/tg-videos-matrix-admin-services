/**
 * 全量构建搜索索引脚本
 * ═══════════════════════
 * 将存量 collections / collection_episodes / media_assets 数据
 * 一次性构建到 search_documents 表中。
 *
 * 用法：
 *   npx ts-node scripts/build-search-index-full.ts
 *
 * 注意事项（§19.2）：
 * - 预估时间：10 万条约 5-10 分钟
 * - 支持断点续传：可用 CURSOR_FILE 环境变量指定断点文件
 * - 建议低峰执行：避免与 catalog_publish 等定时任务冲突
 */
import 'dotenv/config';
import { Prisma, PrismaClient } from '@prisma/client';
import { SearchDocumentBuilder } from '../src/services/search-document-builder';
import type { CollectionInput, EpisodeInput, MediaAssetInput } from '../src/services/search-document-builder';
import { buildSearchText } from '../src/services/search-text-normalizer';
import * as fs from 'fs';
import * as path from 'path';

const prisma = new PrismaClient();
const BATCH_SIZE = 500;
const CURSOR_FILE = process.env.CURSOR_FILE || path.join(__dirname, '.search-index-cursor.json');

interface CursorState {
  phase: 'collections' | 'episodes' | 'assets' | 'done';
  cursor?: string;
  total: number;
  errors: number;
}

function loadCursor(): CursorState {
  try {
    if (fs.existsSync(CURSOR_FILE)) {
      const data = JSON.parse(fs.readFileSync(CURSOR_FILE, 'utf-8'));
      console.log('[full-index] 从断点恢复:', data);
      return data;
    }
  } catch {}
  return { phase: 'collections', total: 0, errors: 0 };
}

function saveCursor(state: CursorState): void {
  fs.writeFileSync(CURSOR_FILE, JSON.stringify(state, null, 2));
}

async function upsertDoc(doc: ReturnType<typeof SearchDocumentBuilder.fromCollection>): Promise<void> {
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
    ext:
      doc.ext == null
        ? Prisma.JsonNull
        : (doc.ext as unknown as Prisma.InputJsonValue),
    sourceUpdatedAt: doc.sourceUpdatedAt,
    indexedAt: doc.indexedAt,
  };

  await prisma.searchDocument.upsert({
    where: { docId: doc.docId },
    create: { docId: doc.docId, ...data },
    update: data,
  });
}

async function buildFullIndex() {
  const state = loadCursor();
  const startTime = Date.now();

  console.log('[full-index] 开始全量构建搜索索引...');
  console.log(`[full-index] 起始阶段: ${state.phase}, 已处理: ${state.total}, 已出错: ${state.errors}`);

  // ═══════ 阶段1: 索引所有已发布的合集 ═══════
  if (state.phase === 'collections') {
    console.log('[full-index] 阶段1: 索引合集...');
    const collections = await prisma.collection.findMany({
      where: { status: 'active' },
      include: { channel: { select: { id: true, tgChatId: true } } },
    });

    for (const collection of collections) {
      try {
        const doc = SearchDocumentBuilder.fromCollection(collection as CollectionInput);
        await upsertDoc(doc);
        state.total++;
      } catch (err) {
        state.errors++;
        console.error(`[full-index] 合集构建失败: ${collection.name}`, err);
      }
    }
    console.log(`[full-index] 合集索引完成: ${collections.length} 条`);
    state.phase = 'episodes';
    state.cursor = undefined;
    saveCursor(state);
  }

  // ═══════ 阶段2: 索引所有已发布的分集 ═══════
  if (state.phase === 'episodes') {
    console.log('[full-index] 阶段2: 索引分集...');
    let cursor = state.cursor ? BigInt(state.cursor) : undefined;

    while (true) {
      const episodes = await prisma.collectionEpisode.findMany({
        where: {
          telegramMessageId: { not: null },
          ...(cursor ? { id: { gt: cursor } } : {}),
        },
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
        orderBy: { id: 'asc' },
        take: BATCH_SIZE,
      });

      if (episodes.length === 0) break;

      for (const ep of episodes) {
        try {
          const doc = SearchDocumentBuilder.fromEpisode(ep as unknown as EpisodeInput);
          await upsertDoc(doc);
          state.total++;
        } catch (err) {
          state.errors++;
        }
      }

      cursor = episodes[episodes.length - 1].id;
      state.cursor = cursor.toString();
      saveCursor(state);
      console.log(`[full-index] 已处理 ${state.total} 条（分集阶段），错误 ${state.errors} 条`);
    }

    state.phase = 'assets';
    state.cursor = undefined;
    saveCursor(state);
  }

  // ═══════ 阶段3: 索引独立视频（非合集） ═══════
  if (state.phase === 'assets') {
    console.log('[full-index] 阶段3: 索引独立视频...');
    let cursor = state.cursor ? BigInt(state.cursor) : undefined;

    while (true) {
      const assets = await prisma.mediaAsset.findMany({
        where: {
          status: 'relay_uploaded',
          collectionEpisode: null,
          ...(cursor ? { id: { gt: cursor } } : {}),
        },
        include: {
          channel: { select: { id: true, tgChatId: true } },
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
        orderBy: { id: 'asc' },
        take: BATCH_SIZE,
      });

      if (assets.length === 0) break;

      for (const asset of assets) {
        if (asset.dispatchTasks.length === 0) continue;
        try {
          const doc = SearchDocumentBuilder.fromMediaAsset(
            asset as unknown as MediaAssetInput,
            asset.dispatchTasks[0],
          );
          await upsertDoc(doc);
          state.total++;
        } catch (err) {
          state.errors++;
        }
      }

      cursor = assets[assets.length - 1].id;
      state.cursor = cursor.toString();
      saveCursor(state);
      console.log(`[full-index] 已处理 ${state.total} 条（独立视频阶段），错误 ${state.errors} 条`);
    }

    state.phase = 'done';
    saveCursor(state);
  }

  // ═══════ 完成 ═══════
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`[full-index] ✅ 全量构建完成！总计 ${state.total} 条，错误 ${state.errors} 条，耗时 ${elapsed} 秒`);

  // 清理断点文件
  try {
    if (fs.existsSync(CURSOR_FILE)) {
      fs.unlinkSync(CURSOR_FILE);
    }
  } catch {}
}

buildFullIndex()
  .catch((err) => {
    console.error('[full-index] 致命错误:', err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

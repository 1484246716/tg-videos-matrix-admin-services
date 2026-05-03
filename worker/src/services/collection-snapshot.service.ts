/**
 * 合集快照增量刷新服务
 * 链路：collection-snapshot.worker -> refreshCollectionSnapshotIncremental -> collection snapshot 表写入
 */

import { TaskStatus } from '@prisma/client';
import {
  COLLECTION_SNAPSHOT_INCREMENTAL_BATCH_SIZE,
} from '../config/env';
import { prisma } from '../infra/prisma';
import { logger } from '../logger';

// 回看窗口：扫描最近1小时内变成success的任务，防止游标跳跃导致遗漏
// 游标按ID推进时可能跳过尚未完成的任务，等这些任务变成success后，
// 回看扫描通过 updatedAt 窗口捕获它们，确保最终一致
const LOOKBACK_WINDOW_MS = 60 * 60 * 1000;

function normalizeCollectionKey(name: string) {
  return name
    .normalize('NFKC')
    .replace(/\s+/g, ' ')
    .trim();
}

function getFileStem(fileName: string) {
  const trimmed = fileName.trim();
  const stem = trimmed.replace(/\.[^./\\]+$/, '').trim();
  return stem || trimmed;
}

function parseCollectionMeta(sourceMeta: unknown) {
  if (!sourceMeta || typeof sourceMeta !== 'object') return null;
  const meta = sourceMeta as Record<string, unknown>;
  if (meta.isCollection !== true) return null;

  const collectionName = typeof meta.collectionName === 'string' ? meta.collectionName : '';
  const episodeNo =
    typeof meta.episodeNo === 'number'
      ? meta.episodeNo
      : typeof meta.episodeNo === 'string' && /^\d+$/.test(meta.episodeNo)
        ? Number(meta.episodeNo)
        : null;

  if (!collectionName || episodeNo === null) return null;

  return { collectionName, episodeNo };
}

// 前瞻扫描 + 回看扫描共用的 select 字段
const DISPATCH_TASK_SELECT = {
  id: true,
  channelId: true,
  finishedAt: true,
  status: true,
  updatedAt: true,
  telegramMessageId: true,
  telegramMessageLink: true,
  mediaAsset: {
    select: {
      originalName: true,
      sourceMeta: true,
    },
  },
} as const;

export async function refreshCollectionSnapshotIncremental() {
  const cursor = await prisma.collectionSnapshotCursor.upsert({
    where: { id: 1 },
    create: { id: 1, lastDispatchId: BigInt(0) },
    update: {},
    select: { id: true, lastDispatchId: true },
  });

  // === 前瞻扫描：按ID顺序从游标位置向后扫描 ===
  // 不再按 status=success 过滤，避免原始Bug：
  // 旧逻辑用 status=success 过滤查询，却用返回结果的最大ID推进游标，
  // 导致中间尚未成功的任务ID被游标跳过，后续即使变成success也无法补录。
  // 例：游标=11291时，11292/11293还是running，查询只返回11294，游标直接跳到11294，
  // 等11292变成success后，下次查 id>11294，11292永远不会再被扫到。
  const rows = await prisma.dispatchTask.findMany({
    where: {
      id: { gt: cursor.lastDispatchId },
    },
    orderBy: { id: 'asc' },
    take: COLLECTION_SNAPSHOT_INCREMENTAL_BATCH_SIZE,
    select: DISPATCH_TASK_SELECT,
  });

  if (rows.length === 0) {
    return { ok: true, scanned: 0, changed: 0, forwardChanged: 0, lookbackChanged: 0 };
  }

  // 游标推进到本批最大ID（不再用safeFrontier停等）
  // 被跳过的活跃任务后续变成success后，由回看扫描兜底补录
  const newCursor = rows[rows.length - 1].id;

  // 前瞻结果中，只有success且已发送的任务才写入快照
  const forwardRows = rows.filter(
    (r) =>
      r.status === TaskStatus.success &&
      r.telegramMessageId !== null,
  );

  // === 回看扫描：捕获最近1小时内变成success的任务 ===
  // 无论任务ID在游标前还是游标后，只要 updatedAt 在窗口内就扫描
  // 这确保了：即使游标曾经跳过某个任务，当它变成success后也能被补录
  const lookbackSince = new Date(Date.now() - LOOKBACK_WINDOW_MS);
  const lookbackRows = await prisma.dispatchTask.findMany({
    where: {
      status: TaskStatus.success,
      telegramMessageId: { not: null },
      updatedAt: { gt: lookbackSince },
    },
    orderBy: { id: 'asc' },
    take: COLLECTION_SNAPSHOT_INCREMENTAL_BATCH_SIZE,
    select: DISPATCH_TASK_SELECT,
  });

  // 合并去重：前瞻 + 回看
  const seenIds = new Set<string>();
  const mergedRows: typeof rows = [];
  for (const r of forwardRows) {
    const key = r.id.toString();
    if (!seenIds.has(key)) {
      seenIds.add(key);
      mergedRows.push(r);
    }
  }
  for (const r of lookbackRows) {
    const key = r.id.toString();
    if (!seenIds.has(key)) {
      seenIds.add(key);
      mergedRows.push(r);
    }
  }
  const lookbackOnlyCount = mergedRows.length - forwardRows.length;

  let changed = 0;
  let forwardChanged = 0;
  let lookbackChanged = 0;
  const forwardIdSet = new Set(forwardRows.map((r) => r.id.toString()));
  const changedHeadKeys = new Set<string>();

  for (const row of mergedRows) {
    const meta = parseCollectionMeta(row.mediaAsset?.sourceMeta);
    if (!meta) continue;

    const collectionNameNormalized = normalizeCollectionKey(meta.collectionName);
    const title = getFileStem(row.mediaAsset?.originalName || '') || `第${meta.episodeNo}集`;

    await prisma.collectionEpisodeSnapshot.upsert({
      where: {
        channelId_collectionNameNormalized_episodeNo: {
          channelId: row.channelId,
          collectionNameNormalized,
          episodeNo: meta.episodeNo,
        },
      },
      create: {
        channelId: row.channelId,
        collectionNameNormalized,
        episodeNo: meta.episodeNo,
        telegramMessageId: row.telegramMessageId,
        telegramMessageUrl: row.telegramMessageLink,
        title,
        isMissingPlaceholder: false,
        sourceDispatchTaskId: row.id,
        sourceUpdatedAt: row.finishedAt,
        snapshotUpdatedAt: new Date(),
      },
      update: {
        telegramMessageId: row.telegramMessageId,
        telegramMessageUrl: row.telegramMessageLink,
        title,
        isMissingPlaceholder: false,
        sourceDispatchTaskId: row.id,
        sourceUpdatedAt: row.finishedAt,
        snapshotUpdatedAt: new Date(),
      },
    });

    changed += 1;
    if (forwardIdSet.has(row.id.toString())) {
      forwardChanged += 1;
    } else {
      lookbackChanged += 1;
    }
    changedHeadKeys.add(`${row.channelId.toString()}::${collectionNameNormalized}`);
  }

  for (const key of changedHeadKeys) {
    const [channelIdRaw, collectionNameNormalized] = key.split('::');
    const channelId = BigInt(channelIdRaw);

    const episodes = await prisma.collectionEpisodeSnapshot.findMany({
      where: { channelId, collectionNameNormalized },
      select: { episodeNo: true, sourceUpdatedAt: true },
      orderBy: { episodeNo: 'asc' },
    });

    const collectionConfig = await prisma.collection.findFirst({
      where: { channelId, nameNormalized: collectionNameNormalized },
      select: { name: true },
    });

    const episodeCount = episodes.length;
    const minEpisodeNo = episodeCount > 0 ? episodes[0].episodeNo : null;
    const maxEpisodeNo = episodeCount > 0 ? episodes[episodeCount - 1].episodeNo : null;
    const lastSourceUpdatedAt = episodes
      .map((item) => item.sourceUpdatedAt)
      .filter((d): d is Date => Boolean(d))
      .sort((a, b) => b.getTime() - a.getTime())[0] ?? null;

    await prisma.collectionSnapshot.upsert({
      where: {
        channelId_collectionNameNormalized: {
          channelId,
          collectionNameNormalized,
        },
      },
      create: {
        channelId,
        collectionName: collectionConfig?.name ?? collectionNameNormalized,
        collectionNameNormalized,
        episodeCount,
        minEpisodeNo,
        maxEpisodeNo,
        lastSourceUpdatedAt,
        lastRebuildAt: new Date(),
        version: BigInt(1),
        isDeleted: false,
      },
      update: {
        collectionName: collectionConfig?.name ?? collectionNameNormalized,
        episodeCount,
        minEpisodeNo,
        maxEpisodeNo,
        lastSourceUpdatedAt,
        lastRebuildAt: new Date(),
        version: { increment: BigInt(1) },
        isDeleted: false,
      },
    });
  }

  // 游标推进到本批最大ID
  await prisma.collectionSnapshotCursor.update({
    where: { id: 1 },
    data: { lastDispatchId: newCursor },
  });

  logger.info('[collection_snapshot] 增量刷新完成', {
    scanned: rows.length,
    changed,
    forwardChanged,
    lookbackChanged,
    lookbackOnlyCount,
    cursorFrom: cursor.lastDispatchId.toString(),
    cursorTo: newCursor.toString(),
  });

  return { ok: true, scanned: rows.length, changed, forwardChanged, lookbackChanged };
}

// 重置快照游标到0，触发全量补录
// 适用场景：历史数据因游标跳跃Bug导致快照缺失，重置后增量刷新会从头扫描所有dispatch任务
// 安全性：快照写入使用upsert（幂等），已有的集会被覆盖但数据不变，缺失的集会被补上
export async function resetCollectionSnapshotCursor() {
  const result = await prisma.collectionSnapshotCursor.update({
    where: { id: 1 },
    data: { lastDispatchId: BigInt(0) },
  });

  logger.info('[collection_snapshot] 游标已重置', {
    lastDispatchId: result.lastDispatchId.toString(),
  });

  return { ok: true, lastDispatchId: result.lastDispatchId.toString() };
}

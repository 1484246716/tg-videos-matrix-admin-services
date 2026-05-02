/**
 * ??????????????????????????????????
 * ?????collection-snapshot.worker -> refreshCollectionSnapshotIncremental -> collection snapshot ??????
 */

import { TaskStatus } from '@prisma/client';
import {
  COLLECTION_SNAPSHOT_INCREMENTAL_BATCH_SIZE,
} from '../config/env';
import { prisma } from '../infra/prisma';
import { logger } from '../logger';

// ??? normalize Collection Key ????????????????????????
function normalizeCollectionKey(name: string) {
  return name
    .normalize('NFKC')
    .replace(/\s+/g, ' ')
    .trim();
}

// ?? get File Stem ?????????????????????
function getFileStem(fileName: string) {
  const trimmed = fileName.trim();
  const stem = trimmed.replace(/\.[^./\\]+$/, '').trim();
  return stem || trimmed;
}

// ?? parse Collection Meta ????????????????????????
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

// ??????????????????????????????
// 任务终态集合：已结束不会再变更的状态
// 修复前Bug：增量刷新用 status=success 过滤查询，却用返回结果的最大ID推进游标，
// 导致中间尚未成功的任务ID被游标跳过，后续即使变成success也无法补录到快照表。
// 例：游标=11291时，11292/11293还是running，查询只返回11294，游标直接跳到11294，
// 等11292变成success后，下次查 id>11294，11292永远不会再被扫到。
const TERMINAL_STATUSES = new Set<TaskStatus>([
  TaskStatus.success,
  TaskStatus.failed,
  TaskStatus.cancelled,
  TaskStatus.dead,
]);

export async function refreshCollectionSnapshotIncremental() {
  const cursor = await prisma.collectionSnapshotCursor.upsert({
    where: { id: 1 },
    create: { id: 1, lastDispatchId: BigInt(0) },
    update: {},
    select: { id: true, lastDispatchId: true },
  });

  // 修复：不再用 status=success 过滤查询，改为查出所有任务后按ID顺序判断
  const rows = await prisma.dispatchTask.findMany({
    where: {
      id: { gt: cursor.lastDispatchId },
    },
    orderBy: { id: 'asc' },
    take: COLLECTION_SNAPSHOT_INCREMENTAL_BATCH_SIZE,
    select: {
      id: true,
      channelId: true,
      finishedAt: true,
      status: true,
      telegramMessageId: true,
      telegramMessageLink: true,
      mediaAsset: {
        select: {
          originalName: true,
          sourceMeta: true,
        },
      },
    },
  });

  if (rows.length === 0) {
    return { ok: true, scanned: 0, changed: 0 };
  }

  // 计算安全前沿：从游标位置开始，按ID顺序遍历，遇到非终态任务就停止
  // 游标只能推进到连续终态任务的最大ID，确保尚未完成的任务不会被跳过
  let safeFrontier = cursor.lastDispatchId;
  for (const row of rows) {
    if (!TERMINAL_STATUSES.has(row.status)) break;
    safeFrontier = row.id;
  }

  // 只处理安全前沿内的success任务（终态中只有success需要写入快照）
  const processableRows = rows.filter(
    (r) =>
      r.id <= safeFrontier &&
      r.status === TaskStatus.success &&
      r.telegramMessageId !== null,
  );

  let changed = 0;
  const changedHeadKeys = new Set<string>();

  for (const row of processableRows) {
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

  // 游标推进到安全前沿而非返回结果的最大ID，防止跳过尚未完成的任务
  await prisma.collectionSnapshotCursor.update({
    where: { id: 1 },
    data: { lastDispatchId: safeFrontier },
  });

  logger.info('[collection_snapshot] 增量刷新完成', {
    scanned: rows.length,
    changed,
    cursorFrom: cursor.lastDispatchId.toString(),
    cursorTo: safeFrontier.toString(),
    safeFrontier: safeFrontier.toString(),
  });

  return { ok: true, scanned: rows.length, changed };
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

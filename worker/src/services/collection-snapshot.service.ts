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
export async function refreshCollectionSnapshotIncremental() {
  const cursor = await prisma.collectionSnapshotCursor.upsert({
    where: { id: 1 },
    create: { id: 1, lastDispatchId: BigInt(0) },
    update: {},
    select: { id: true, lastDispatchId: true },
  });

  const rows = await prisma.dispatchTask.findMany({
    where: {
      id: { gt: cursor.lastDispatchId },
      status: TaskStatus.success,
      telegramMessageId: { not: null },
    },
    orderBy: { id: 'asc' },
    take: COLLECTION_SNAPSHOT_INCREMENTAL_BATCH_SIZE,
    select: {
      id: true,
      channelId: true,
      finishedAt: true,
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

  let changed = 0;
  const changedHeadKeys = new Set<string>();

  for (const row of rows) {
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

  const lastId = rows[rows.length - 1].id;
  await prisma.collectionSnapshotCursor.update({
    where: { id: 1 },
    data: { lastDispatchId: lastId },
  });

  logger.info('[collection_snapshot] 增量刷新完成', {
    scanned: rows.length,
    changed,
    cursorFrom: cursor.lastDispatchId.toString(),
    cursorTo: lastId.toString(),
  });

  return { ok: true, scanned: rows.length, changed };
}

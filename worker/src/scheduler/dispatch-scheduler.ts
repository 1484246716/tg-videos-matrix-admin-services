import { MediaStatus, TaskStatus } from '@prisma/client';
import {
  DISPATCH_CHANNEL_INTERVAL_GUARD_ENABLED,
  MAX_SCHEDULE_BATCH,
} from '../config/env';
import { prisma, getTaskDefinitionModel } from '../infra/prisma';
import { dispatchQueue } from '../infra/redis';
import { logger } from '../logger';
import { releaseChannelLock, tryAcquireChannelLock } from '../shared/channel-lock';
import { updateTaskDefinitionRunStatus } from '../services/task-definition.service';

const DISPATCH_HEAD_BYPASS_RETRY_THRESHOLD = 2;
const DISPATCH_HEAD_BYPASS_DELAY_SEC = 10 * 60;
const COLLECTION_SKIP_GRACE_MS = 5 * 60 * 1000;
const COLLECTION_SKIP_REASON = 'auto_skip_missing_after_grace';

function computeDispatchNextAllowedAt(args: {
  lastPostAt: Date | null;
  postIntervalSec: number;
  now: Date;
}) {
  if (!args.lastPostAt) return args.now;
  return new Date(args.lastPostAt.getTime() + Math.max(0, args.postIntervalSec) * 1000);
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
  const episodeParseFailed = meta.episodeParseFailed === true;

  if (!collectionName) return null;
  return {
    collectionName,
    episodeNo,
    episodeParseFailed,
  };
}

function parseCollectionSchedulerConfig(navReplyMarkup: unknown) {
  const fallback = {
    collectionDispatchGateEnabled: true,
    collectionHeadBypassEnabled: false,
    collectionHeadBypassMinutes: 180,
  };

  if (!navReplyMarkup || typeof navReplyMarkup !== 'object' || Array.isArray(navReplyMarkup)) {
    return fallback;
  }

  const root = navReplyMarkup as Record<string, unknown>;
  const cfgRaw = root.__collectionConfig;
  if (!cfgRaw || typeof cfgRaw !== 'object' || Array.isArray(cfgRaw)) {
    return fallback;
  }

  const cfg = cfgRaw as Record<string, unknown>;
  const collectionDispatchGateEnabled =
    typeof cfg.collectionDispatchGateEnabled === 'boolean'
      ? cfg.collectionDispatchGateEnabled
      : fallback.collectionDispatchGateEnabled;
  const collectionHeadBypassEnabled =
    typeof cfg.collectionHeadBypassEnabled === 'boolean'
      ? cfg.collectionHeadBypassEnabled
      : fallback.collectionHeadBypassEnabled;
  const collectionHeadBypassMinutes =
    typeof cfg.collectionHeadBypassMinutes === 'number' && cfg.collectionHeadBypassMinutes > 0
      ? Math.floor(cfg.collectionHeadBypassMinutes)
      : fallback.collectionHeadBypassMinutes;

  return {
    collectionDispatchGateEnabled,
    collectionHeadBypassEnabled,
    collectionHeadBypassMinutes,
  };
}

async function canDispatchCollectionEpisode(args: {
  channelId: bigint;
  collectionName: string;
  episodeNo: number;
  now: Date;
}) {
  const collectionAssets = await prisma.mediaAsset.findMany({
    where: {
      channelId: args.channelId,
      sourceMeta: {
        path: ['collectionName'],
        equals: args.collectionName,
      },
    },
    select: {
      id: true,
      sourceMeta: true,
      status: true,
      updatedAt: true,
    },
  });

  const prevEpisodes = collectionAssets
    .map((asset) => ({ id: asset.id, meta: parseCollectionMeta(asset.sourceMeta), status: asset.status, updatedAt: asset.updatedAt }))
    .filter((item) => item.meta && !item.meta.episodeParseFailed && item.meta.episodeNo !== null)
    .filter((item) => (item.meta?.episodeNo ?? 0) < args.episodeNo) as Array<{
    id: bigint;
    meta: { collectionName: string; episodeNo: number | null; episodeParseFailed: boolean };
    status: MediaStatus;
    updatedAt: Date;
  }>;

  if (prevEpisodes.length === 0) {
    return { allowed: true as const, blockedByEpisodeNo: null as number | null };
  }

  const prevEpisodeAssetIds = prevEpisodes.map((item) => item.id);

  const successRows = await prisma.dispatchTask.findMany({
    where: {
      mediaAssetId: { in: prevEpisodeAssetIds },
      status: TaskStatus.success,
    },
    select: { mediaAssetId: true },
    distinct: ['mediaAssetId'],
  });

  const successSet = new Set(successRows.map((row) => row.mediaAssetId.toString()));

  for (const prev of prevEpisodes) {
    if (successSet.has(prev.id.toString())) continue;

    const isFailedOrDeleted = prev.status === MediaStatus.failed || prev.status === MediaStatus.deleted;
    if (!isFailedOrDeleted) continue;

    const elapsedMs = args.now.getTime() - prev.updatedAt.getTime();
    if (Number.isFinite(elapsedMs) && elapsedMs >= COLLECTION_SKIP_GRACE_MS) {
      const sourceMeta = collectionAssets.find((asset) => asset.id === prev.id)?.sourceMeta;
      const metaObj = sourceMeta && typeof sourceMeta === 'object'
        ? (sourceMeta as Record<string, unknown>)
        : {};

      await prisma.mediaAsset.update({
        where: { id: prev.id },
        data: {
          sourceMeta: {
            ...metaObj,
            skipStatus: 'skipped_missing',
            skipReason: COLLECTION_SKIP_REASON,
            skipAt: args.now.toISOString(),
          },
        },
      });
    }
  }

  const refreshedAssets = await prisma.mediaAsset.findMany({
    where: { id: { in: prevEpisodeAssetIds } },
    select: { id: true, sourceMeta: true },
  });

  const skippedSet = new Set(
    refreshedAssets
      .filter((asset) => {
        const meta = asset.sourceMeta && typeof asset.sourceMeta === 'object'
          ? (asset.sourceMeta as Record<string, unknown>)
          : {};
        return meta.skipStatus === 'skipped_missing';
      })
      .map((asset) => asset.id.toString()),
  );

  const blocked = prevEpisodes
    .filter((item) => !successSet.has(item.id.toString()) && !skippedSet.has(item.id.toString()))
    .sort((a, b) => (a.meta.episodeNo ?? 0) - (b.meta.episodeNo ?? 0))[0];

  if (!blocked) {
    return { allowed: true as const, blockedByEpisodeNo: null as number | null };
  }

  return {
    allowed: false as const,
    blockedByEpisodeNo: blocked.meta.episodeNo ?? null,
  };
}

export async function scheduleDueDispatchTasks() {
  const now = new Date();

  const dueTasks = await prisma.dispatchTask.findMany({
    where: {
      status: { in: [TaskStatus.pending, TaskStatus.scheduled, TaskStatus.failed] },
      nextRunAt: { lte: now },
    },
    orderBy: [{ priority: 'asc' }, { nextRunAt: 'asc' }],
    take: MAX_SCHEDULE_BATCH,
    select: {
      id: true,
      status: true,
      channelId: true,
      mediaAssetId: true,
      retryCount: true,
      maxRetries: true,
      nextRunAt: true,
      channel: {
        select: {
          postIntervalSec: true,
          lastPostAt: true,
          navReplyMarkup: true,
        },
      },
      mediaAsset: {
        select: {
          sourceMeta: true,
        },
      },
    },
  });

  const queuedChannelIds = new Set<string>();
  let queuedCount = 0;

  for (const task of dueTasks) {
    const channelIdStr = task.channelId.toString();

    if (queuedChannelIds.has(channelIdStr)) {
      continue;
    }

    const collectionMeta = parseCollectionMeta(task.mediaAsset.sourceMeta);
    const collectionCfg = parseCollectionSchedulerConfig(task.channel.navReplyMarkup);

    const shouldBypassHead =
      !collectionMeta &&
      task.status === TaskStatus.failed &&
      task.retryCount >= DISPATCH_HEAD_BYPASS_RETRY_THRESHOLD &&
      task.retryCount < task.maxRetries;

    if (shouldBypassHead) {
      const bypassNextRunAt = new Date(Date.now() + DISPATCH_HEAD_BYPASS_DELAY_SEC * 1000);
      await prisma.dispatchTask.update({
        where: { id: task.id },
        data: {
          status: TaskStatus.failed,
          nextRunAt: bypassNextRunAt,
        },
      });

      logger.warn('[scheduler] 分发头阻塞旁路，临时延后高重试任务', {
        taskId: task.id.toString(),
        channelId: channelIdStr,
        retryCount: task.retryCount,
        maxRetries: task.maxRetries,
        bypassNextRunAt: bypassNextRunAt.toISOString(),
      });

      continue;
    }

    if (collectionMeta?.episodeParseFailed) {
      await prisma.dispatchTask.update({
        where: { id: task.id },
        data: {
          status: TaskStatus.scheduled,
          nextRunAt: new Date(Date.now() + 10 * 60 * 1000),
        },
      });

      logger.warn('[scheduler] 合集集号解析失败，阻塞分发等待人工改名重扫', {
        taskId: task.id.toString(),
        channelId: channelIdStr,
        mediaAssetId: task.mediaAssetId.toString(),
        collectionName: collectionMeta.collectionName,
      });
      continue;
    }

    if (
      collectionCfg.collectionDispatchGateEnabled &&
      collectionMeta &&
      !collectionMeta.episodeParseFailed &&
      collectionMeta.episodeNo !== null
    ) {
      const gateResult = await canDispatchCollectionEpisode({
        channelId: task.channelId,
        collectionName: collectionMeta.collectionName,
        episodeNo: collectionMeta.episodeNo,
        now,
      });

      if (!gateResult.allowed) {
        const bypassReady =
          collectionCfg.collectionHeadBypassEnabled &&
          task.status === TaskStatus.failed &&
          task.retryCount >= DISPATCH_HEAD_BYPASS_RETRY_THRESHOLD;

        if (bypassReady) {
          const bypassNextRunAt = new Date(
            Date.now() + collectionCfg.collectionHeadBypassMinutes * 60 * 1000,
          );
          await prisma.dispatchTask.update({
            where: { id: task.id },
            data: {
              status: TaskStatus.failed,
              nextRunAt: bypassNextRunAt,
            },
          });

          logger.warn('[scheduler] 合集头部阻塞旁路生效，延后重试', {
            taskId: task.id.toString(),
            channelId: channelIdStr,
            mediaAssetId: task.mediaAssetId.toString(),
            collectionName: collectionMeta.collectionName,
            episodeNo: collectionMeta.episodeNo,
            blockedByEpisodeNo: gateResult.blockedByEpisodeNo,
            collectionHeadBypassMinutes: collectionCfg.collectionHeadBypassMinutes,
            bypassNextRunAt: bypassNextRunAt.toISOString(),
          });
          continue;
        }

        await prisma.dispatchTask.update({
          where: { id: task.id },
          data: {
            status: TaskStatus.scheduled,
            nextRunAt: new Date(Date.now() + 60 * 1000),
          },
        });

        logger.info('[scheduler] 合集顺序闸门阻塞当前分发任务', {
          taskId: task.id.toString(),
          channelId: channelIdStr,
          mediaAssetId: task.mediaAssetId.toString(),
          collectionName: collectionMeta.collectionName,
          episodeNo: collectionMeta.episodeNo,
          blockedByEpisodeNo: gateResult.blockedByEpisodeNo,
        });
        continue;
      }
    }

    if (DISPATCH_CHANNEL_INTERVAL_GUARD_ENABLED) {
      const nextAllowedAt = computeDispatchNextAllowedAt({
        lastPostAt: task.channel.lastPostAt,
        postIntervalSec: task.channel.postIntervalSec,
        now,
      });

      if (nextAllowedAt.getTime() > now.getTime()) {
        await prisma.dispatchTask.update({
          where: { id: task.id },
          data: {
            status: TaskStatus.scheduled,
            nextRunAt: nextAllowedAt,
          },
        });

        logger.info('[scheduler] 分发任务未到频道发送窗口，已延后', {
          taskId: task.id.toString(),
          channelId: channelIdStr,
          postIntervalSec: task.channel.postIntervalSec,
          lastPostAt: task.channel.lastPostAt?.toISOString() ?? null,
          nextAllowedAt: nextAllowedAt.toISOString(),
        });
        continue;
      }
    }

    const lock = await tryAcquireChannelLock({
      scope: 'dispatch',
      channelId: task.channelId,
    });

    if (!lock.acquired) {
      logger.info('[scheduler] 分发任务跳过（频道锁未获取）', {
        taskId: task.id.toString(),
        channelId: channelIdStr,
        lockKey: lock.lockKey,
      });
      continue;
    }

    try {
      const updated = await prisma.dispatchTask.updateMany({
        where: {
          id: task.id,
          status: {
            in: [TaskStatus.pending, TaskStatus.scheduled, TaskStatus.failed],
          },
        },
        data: {
          status: TaskStatus.scheduled,
        },
      });

      if (updated.count === 0) continue;

      await dispatchQueue.add(
        'dispatch-send',
        {
          dispatchTaskId: task.id.toString(),
          channelId: task.channelId.toString(),
          mediaAssetId: task.mediaAssetId.toString(),
          retryCount: task.retryCount,
        },
        {
          jobId: `dispatch-${task.id.toString()}`,
          removeOnComplete: true,
          removeOnFail: 200,
        },
      );

      queuedChannelIds.add(channelIdStr);
      queuedCount += 1;
    } finally {
      await releaseChannelLock({ lockKey: lock.lockKey, lockToken: lock.lockToken });
    }
  }

  if (queuedCount > 0) {
    logger.info('[scheduler] 已入队分发任务', { count: queuedCount });
  }
}

export async function scheduleDispatchForDefinition(taskDefinitionId: bigint) {
  try {
    const definition = await getTaskDefinitionModel().findUnique({
      where: { id: taskDefinitionId },
      select: { priority: true },
    });

    if (!definition) {
      throw new Error(`未找到任务定义: ${taskDefinitionId}`);
    }

    const unscheduledAssets = await prisma.mediaAsset.findMany({
      where: {
        status: MediaStatus.relay_uploaded,
        telegramFileId: { not: null },
        dispatchTasks: {
          none: {},
        },
      },
      select: {
        id: true,
        channelId: true,
      },
      take: 200,
    });

    let createdCount = 0;
    const now = new Date();

    for (const asset of unscheduledAssets) {
      await prisma.dispatchTask.create({
        data: {
          channelId: asset.channelId,
          mediaAssetId: asset.id,
          status: TaskStatus.pending,
          scheduleSlot: now,
          plannedAt: now,
          nextRunAt: now,
          priority: definition.priority ?? 100,
        },
        select: { id: true },
      });

      createdCount += 1;
    }

    await scheduleDueDispatchTasks();

    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'success',
      summary: {
        executor: 'dispatch_send',
        createdTasks: createdCount,
        message: '自动扫描并入队分发任务',
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    // eslint-disable-next-line no-console
    console.error(`[scheduler] 分发调度失败，taskDef=${taskDefinitionId}:`, error);
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'failed',
      summary: { executor: 'dispatch_send', error: `分发调度失败: ${message}` },
    });
  }
}

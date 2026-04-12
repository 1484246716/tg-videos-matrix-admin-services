import { MediaStatus, TaskStatus } from '@prisma/client';
import {
  COLLECTION_AUTO_BYPASS_AFTER_MINUTES,
  COLLECTION_AUTO_BYPASS_ENABLED,
  COLLECTION_AUTO_BYPASS_MARK,
  COLLECTION_AUTO_BYPASS_ON_MAX_RETRIES,
  DISPATCH_CHANNEL_INTERVAL_GUARD_ENABLED,
  MAX_SCHEDULE_BATCH,
} from '../config/env';
import { prisma, getTaskDefinitionModel, withPrismaRetry } from '../infra/prisma';
import { dispatchQueue } from '../infra/redis';
import { logger } from '../logger';
import { releaseChannelLock, tryAcquireChannelLock } from '../shared/channel-lock';
import { updateTaskDefinitionRunStatus } from '../services/task-definition.service';

const DISPATCH_HEAD_BYPASS_RETRY_THRESHOLD = 2;
const DISPATCH_HEAD_BYPASS_DELAY_SEC = 10 * 60;
const COLLECTION_SKIP_GRACE_MS = 5 * 60 * 1000;
const COLLECTION_SKIP_REASON = 'auto_skip_missing_after_grace';

type CollectionGateEvalStats = {
  autoBypassCount: number;
  blockedCount: number;
};

function parseEpisodeNoFromText(text: string) {
  const patterns = [
    /\[第\s*(\d+)\s*(?:集|话|話)\]/,
    /第\s*(\d+)\s*(?:集|话|話)/,
    /S\d+E(\d+)/i,
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match || !match[1]) continue;
    const parsed = Number(match[1]);
    if (!Number.isFinite(parsed) || parsed <= 0) continue;
    return parsed;
  }
  return null;
}

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
  stats?: CollectionGateEvalStats;
}) {
  const collectionAssets = await withPrismaRetry(
    () =>
      prisma.mediaAsset.findMany({
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
      }),
    { label: 'dispatch.canDispatchCollectionEpisode.collectionAssets' },
  );

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

  const successRows = await withPrismaRetry(
    () =>
      prisma.dispatchTask.findMany({
        where: {
          mediaAssetId: { in: prevEpisodeAssetIds },
          status: TaskStatus.success,
        },
        select: { mediaAssetId: true },
        distinct: ['mediaAssetId'],
      }),
    { label: 'dispatch.canDispatchCollectionEpisode.successRows' },
  );

  const successSet = new Set(successRows.map((row) => row.mediaAssetId.toString()));

  const latestDispatchRows = await withPrismaRetry(
    () =>
      prisma.dispatchTask.findMany({
        where: {
          mediaAssetId: { in: prevEpisodeAssetIds },
        },
        orderBy: [{ mediaAssetId: 'asc' }, { id: 'desc' }],
        select: {
          id: true,
          mediaAssetId: true,
          status: true,
          retryCount: true,
          maxRetries: true,
          updatedAt: true,
        },
      }),
    { label: 'dispatch.canDispatchCollectionEpisode.latestDispatchRows' },
  );

  const latestDispatchMap = new Map<string, {
    id: bigint;
    status: TaskStatus;
    retryCount: number;
    maxRetries: number;
    updatedAt: Date;
  }>();

  for (const row of latestDispatchRows) {
    const key = row.mediaAssetId.toString();
    if (latestDispatchMap.has(key)) continue;
    latestDispatchMap.set(key, {
      id: row.id,
      status: row.status,
      retryCount: row.retryCount,
      maxRetries: row.maxRetries,
      updatedAt: row.updatedAt,
    });
  }

  for (const prev of prevEpisodes) {
    if (successSet.has(prev.id.toString())) continue;

    const assetIdStr = prev.id.toString();
    const latestDispatch = latestDispatchMap.get(assetIdStr);
    const latestDispatchElapsedMs = latestDispatch
      ? args.now.getTime() - latestDispatch.updatedAt.getTime()
      : null;

    if (COLLECTION_AUTO_BYPASS_ENABLED) {
      const hitMaxRetries =
        COLLECTION_AUTO_BYPASS_ON_MAX_RETRIES &&
        latestDispatch &&
        latestDispatch.status !== TaskStatus.success &&
        latestDispatch.retryCount >= latestDispatch.maxRetries;

      const hitTimeout =
        latestDispatch &&
        latestDispatch.status !== TaskStatus.success &&
        Number.isFinite(latestDispatchElapsedMs) &&
        (latestDispatchElapsedMs as number) >= COLLECTION_AUTO_BYPASS_AFTER_MINUTES * 60 * 1000;

      if (hitMaxRetries || hitTimeout) {
        const sourceMeta = collectionAssets.find((asset) => asset.id === prev.id)?.sourceMeta;
        const metaObj = sourceMeta && typeof sourceMeta === 'object'
          ? (sourceMeta as Record<string, unknown>)
          : {};

        await withPrismaRetry(
          () =>
            prisma.mediaAsset.update({
              where: { id: prev.id },
              data: {
                sourceMeta: {
                  ...metaObj,
                  skipStatus: COLLECTION_AUTO_BYPASS_MARK,
                  skipReason: 'auto_bypass_after_retry_or_timeout',
                  skipAt: args.now.toISOString(),
                },
              },
            }),
          { label: 'dispatch.collection.autoBypass.updateMediaAsset' },
        );

        await withPrismaRetry(
          () =>
            prisma.riskEvent.create({
              data: {
                level: 'medium',
                eventType: 'collection_episode_auto_bypass',
                channelId: args.channelId,
                dispatchTaskId: latestDispatch?.id,
                payload: {
                  collectionName: args.collectionName,
                  blockedByEpisodeNo: prev.meta.episodeNo,
                  autoBypassAfterMinutes: COLLECTION_AUTO_BYPASS_AFTER_MINUTES,
                  reason: hitMaxRetries ? 'max_retries_exhausted' : 'timeout_exceeded',
                },
              },
            }),
          { label: 'dispatch.collection.autoBypass.riskEvent' },
        );

        if (args.stats) args.stats.autoBypassCount += 1;

        logger.warn('[scheduler] 合集顺序闸门自动旁路前置集', {
          channelId: args.channelId.toString(),
          collectionName: args.collectionName,
          blockedByEpisodeNo: prev.meta.episodeNo,
          mediaAssetId: assetIdStr,
          reason: hitMaxRetries ? 'max_retries_exhausted' : 'timeout_exceeded',
          collection_gate_auto_bypass_total: 1,
          collectionAutoBypassEnabled: COLLECTION_AUTO_BYPASS_ENABLED,
          collectionAutoBypassAfterMinutes: COLLECTION_AUTO_BYPASS_AFTER_MINUTES,
          collectionAutoBypassOnMaxRetries: COLLECTION_AUTO_BYPASS_ON_MAX_RETRIES,
        });

        continue;
      }
    }

    const isFailedOrDeleted = prev.status === MediaStatus.failed || prev.status === MediaStatus.deleted;
    if (!isFailedOrDeleted) continue;

    const elapsedMs = args.now.getTime() - prev.updatedAt.getTime();
    if (Number.isFinite(elapsedMs) && elapsedMs >= COLLECTION_SKIP_GRACE_MS) {
      const sourceMeta = collectionAssets.find((asset) => asset.id === prev.id)?.sourceMeta;
      const metaObj = sourceMeta && typeof sourceMeta === 'object'
        ? (sourceMeta as Record<string, unknown>)
        : {};

      await withPrismaRetry(
        () =>
          prisma.mediaAsset.update({
            where: { id: prev.id },
            data: {
              sourceMeta: {
                ...metaObj,
                skipStatus: 'skipped_missing',
                skipReason: COLLECTION_SKIP_REASON,
                skipAt: args.now.toISOString(),
              },
            },
          }),
        { label: 'dispatch.collection.skipMissing.updateMediaAsset' },
      );
    }
  }

  const refreshedAssets = await withPrismaRetry(
    () =>
      prisma.mediaAsset.findMany({
        where: { id: { in: prevEpisodeAssetIds } },
        select: { id: true, sourceMeta: true },
      }),
    { label: 'dispatch.canDispatchCollectionEpisode.refreshedAssets' },
  );

  const skippedSet = new Set(
    refreshedAssets
      .filter((asset) => {
        const meta = asset.sourceMeta && typeof asset.sourceMeta === 'object'
          ? (asset.sourceMeta as Record<string, unknown>)
          : {};
        return meta.skipStatus === 'skipped_missing' || meta.skipStatus === COLLECTION_AUTO_BYPASS_MARK;
      })
      .map((asset) => asset.id.toString()),
  );

  const blocked = prevEpisodes
    .filter((item) => !successSet.has(item.id.toString()) && !skippedSet.has(item.id.toString()))
    .sort((a, b) => (a.meta.episodeNo ?? 0) - (b.meta.episodeNo ?? 0))[0];

  if (!blocked) {
    return { allowed: true as const, blockedByEpisodeNo: null as number | null };
  }

  if (args.stats) args.stats.blockedCount += 1;

  return {
    allowed: false as const,
    blockedByEpisodeNo: blocked.meta.episodeNo ?? null,
  };
}

export async function scheduleDueDispatchTasks() {
  const now = new Date();
  const gateStats: CollectionGateEvalStats = {
    autoBypassCount: 0,
    blockedCount: 0,
  };

  const dueTasks = await withPrismaRetry(
    () =>
      prisma.dispatchTask.findMany({
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
              originalName: true,
            },
          },
        },
      }),
    { label: 'dispatch.scheduleDueDispatchTasks.findDueTasks' },
  );

  const queuedChannelIds = new Set<string>();
  let queuedCount = 0;

  for (const task of dueTasks) {
    const channelIdStr = task.channelId.toString();

    if (queuedChannelIds.has(channelIdStr)) {
      continue;
    }

    let collectionMeta = parseCollectionMeta(task.mediaAsset.sourceMeta);
    const collectionCfg = parseCollectionSchedulerConfig(task.channel.navReplyMarkup);

    if (collectionMeta?.episodeParseFailed || (collectionMeta && collectionMeta.episodeNo === null)) {
      const fallbackEpisodeNo = parseEpisodeNoFromText(task.mediaAsset.originalName || '');
      if (fallbackEpisodeNo !== null) {
        const sourceMetaRaw = task.mediaAsset.sourceMeta;
        const sourceMetaObj =
          sourceMetaRaw && typeof sourceMetaRaw === 'object'
            ? (sourceMetaRaw as Record<string, unknown>)
            : {};

        await withPrismaRetry(
          () =>
            prisma.mediaAsset.update({
              where: { id: task.mediaAssetId },
              data: {
                sourceMeta: {
                  ...sourceMetaObj,
                  episodeNo: fallbackEpisodeNo,
                  episodeParseFailed: false,
                },
              },
            }),
          { label: 'dispatch.scheduleDueDispatchTasks.fixEpisodeNo' },
        );

        collectionMeta = {
          collectionName: collectionMeta.collectionName,
          episodeNo: fallbackEpisodeNo,
          episodeParseFailed: false,
        };

        logger.info('[scheduler] 合集集号已从文件名自动修复', {
          taskId: task.id.toString(),
          channelId: channelIdStr,
          mediaAssetId: task.mediaAssetId.toString(),
          collectionName: collectionMeta.collectionName,
          episodeNo: fallbackEpisodeNo,
          originalName: task.mediaAsset.originalName,
        });
      }
    }

    const shouldBypassHead =
      !collectionMeta &&
      task.status === TaskStatus.failed &&
      task.retryCount >= DISPATCH_HEAD_BYPASS_RETRY_THRESHOLD &&
      task.retryCount < task.maxRetries;

    if (shouldBypassHead) {
      const bypassNextRunAt = new Date(Date.now() + DISPATCH_HEAD_BYPASS_DELAY_SEC * 1000);
      await withPrismaRetry(
        () =>
          prisma.dispatchTask.update({
            where: { id: task.id },
            data: {
              status: TaskStatus.failed,
              nextRunAt: bypassNextRunAt,
            },
          }),
        { label: 'dispatch.scheduleDueDispatchTasks.bypassHead' },
      );

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
      await withPrismaRetry(
        () =>
          prisma.dispatchTask.update({
            where: { id: task.id },
            data: {
              status: TaskStatus.scheduled,
              nextRunAt: new Date(Date.now() + 10 * 60 * 1000),
            },
          }),
        { label: 'dispatch.scheduleDueDispatchTasks.episodeParseFailedDelay' },
      );

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
        stats: gateStats,
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
          await withPrismaRetry(
            () =>
              prisma.dispatchTask.update({
                where: { id: task.id },
                data: {
                  status: TaskStatus.failed,
                  nextRunAt: bypassNextRunAt,
                },
              }),
            { label: 'dispatch.scheduleDueDispatchTasks.collectionBypassHead' },
          );

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

        await withPrismaRetry(
          () =>
            prisma.dispatchTask.update({
              where: { id: task.id },
              data: {
                status: TaskStatus.scheduled,
                nextRunAt: new Date(Date.now() + 60 * 1000),
              },
            }),
          { label: 'dispatch.scheduleDueDispatchTasks.collectionGateDelay' },
        );

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
        await withPrismaRetry(
          () =>
            prisma.dispatchTask.update({
              where: { id: task.id },
              data: {
                status: TaskStatus.scheduled,
                nextRunAt: nextAllowedAt,
              },
            }),
          { label: 'dispatch.scheduleDueDispatchTasks.channelWindowDelay' },
        );

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
      const updated = await withPrismaRetry(
        () =>
          prisma.dispatchTask.updateMany({
            where: {
              id: task.id,
              status: {
                in: [TaskStatus.pending, TaskStatus.scheduled, TaskStatus.failed],
              },
            },
            data: {
              status: TaskStatus.scheduled,
            },
          }),
        { label: 'dispatch.scheduleDueDispatchTasks.markScheduled' },
      );

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

  logger.info('[typeb_metrics] dispatch scheduler tick', {
    typeb_enqueue_total: queuedCount,
    collection_gate_auto_bypass_total: gateStats.autoBypassCount,
    collection_gate_block_total: gateStats.blockedCount,
    collectionAutoBypassEnabled: COLLECTION_AUTO_BYPASS_ENABLED,
    collectionAutoBypassAfterMinutes: COLLECTION_AUTO_BYPASS_AFTER_MINUTES,
    collectionAutoBypassOnMaxRetries: COLLECTION_AUTO_BYPASS_ON_MAX_RETRIES,
  });

  if (queuedCount > 0) {
    logger.info('[scheduler] 已入队分发任务', { count: queuedCount });
  }

  return {
    queuedCount,
    autoBypassCount: gateStats.autoBypassCount,
    blockedCount: gateStats.blockedCount,
  };
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

    const unscheduledAssets = await withPrismaRetry(
      () =>
        prisma.mediaAsset.findMany({
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
            originalName: true,
            sourceMeta: true,
          },
          take: 200,
        }),
      { label: 'dispatch.scheduleDispatchForDefinition.findUnscheduledAssets' },
    );

    let createdCount = 0;
    let groupScheduledCount = 0;
    const now = new Date();

    const grouped = new Map<string, typeof unscheduledAssets>();
    for (const asset of unscheduledAssets) {
      const sourceMeta = (asset.sourceMeta ?? {}) as Record<string, unknown>;
      const groupKeyRaw = sourceMeta.groupKey;
      const groupKey =
        typeof groupKeyRaw === 'string' && groupKeyRaw.trim()
          ? groupKeyRaw.trim()
          : `asset-${asset.id.toString()}`;
      const key = `${asset.channelId.toString()}::${groupKey}`;
      const bucket = grouped.get(key);
      if (bucket) bucket.push(asset);
      else grouped.set(key, [asset]);
    }

    for (const [key, assets] of grouped) {
      const [, groupKey] = key.split('::');
      const sorted = [...assets].sort((a, b) =>
        String(a.originalName || '').localeCompare(String(b.originalName || ''), 'zh-CN'),
      );
      if (sorted.length > 1) {
        groupScheduledCount += 1;
      }

      for (let i = 0; i < sorted.length; i += 1) {
        const asset = sorted[i];
        const slot = new Date(now.getTime() + i * 1000);

        await withPrismaRetry(
          () =>
            prisma.dispatchTask.create({
              data: {
                channelId: asset.channelId,
                mediaAssetId: asset.id,
                groupKey,
                status: TaskStatus.pending,
                scheduleSlot: slot,
                plannedAt: slot,
                nextRunAt: slot,
                priority: definition.priority ?? 100,
              },
              select: { id: true },
            }),
          { label: 'dispatch.scheduleDispatchForDefinition.createDispatchTask' },
        );

        createdCount += 1;
      }
    }

    const tickSummary = await scheduleDueDispatchTasks();

    logger.info('[typeb_metrics] grouped scheduling summary', {
      typeb_group_scheduled_total: groupScheduledCount,
      typeb_group_asset_created_total: createdCount,
      metric_labels: {
        typeb_group_scheduled_total: 'TypeB 组级调度批次数',
        typeb_group_asset_created_total: 'TypeB 组级调度创建任务总数',
      },
    });

    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'success',
      summary: {
        executor: 'dispatch_send',
        createdTasks: createdCount,
        groupedBatches: groupScheduledCount,
        queuedCount: tickSummary.queuedCount,
        autoBypassCount: tickSummary.autoBypassCount,
        blockedCount: tickSummary.blockedCount,
        collectionAutoBypassEnabled: COLLECTION_AUTO_BYPASS_ENABLED,
        collectionAutoBypassAfterMinutes: COLLECTION_AUTO_BYPASS_AFTER_MINUTES,
        collectionAutoBypassOnMaxRetries: COLLECTION_AUTO_BYPASS_ON_MAX_RETRIES,
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

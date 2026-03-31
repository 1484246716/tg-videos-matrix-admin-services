import { MediaStatus, TaskStatus } from '@prisma/client';
import {
  DISPATCH_CHANNEL_INTERVAL_GUARD_ENABLED,
  MAX_SCHEDULE_BATCH,
} from '../config/env';
import { prisma, getTaskDefinitionModel } from '../infra/prisma';
import { dispatchQueue } from '../infra/redis';
import { logger } from '../logger';
import { releaseChannelLock, tryAcquireChannelLock } from '../shared/channel-lock';
import {
  buildCollectionOrderMeta,
  buildNormalOrderMeta,
  parseEpisodeOrderFromText,
  parseOrderSchedulerConfig,
  resolveCollectionDispatchOrderConfig,
  resolveOrderMeta,
} from '../shared/order-meta';
import { updateTaskDefinitionRunStatus } from '../services/task-definition.service';

const DISPATCH_HEAD_BYPASS_RETRY_THRESHOLD = 2;
const COLLECTION_SKIP_GRACE_MS = 5 * 60 * 1000;
const COLLECTION_SKIP_REASON = 'auto_skip_missing_after_grace';

function asObject(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, unknown>;
}

function computeDispatchNextAllowedAt(args: {
  lastPostAt: Date | null;
  postIntervalSec: number;
  now: Date;
}) {
  if (!args.lastPostAt) return args.now;
  return new Date(args.lastPostAt.getTime() + Math.max(0, args.postIntervalSec) * 1000);
}

async function loadDueDispatchTasks(now: Date) {
  return prisma.dispatchTask.findMany({
    where: {
      status: { in: [TaskStatus.pending, TaskStatus.scheduled, TaskStatus.failed] },
      nextRunAt: { lte: now },
    },
    orderBy: [{ priority: 'asc' }, { nextRunAt: 'asc' }, { id: 'asc' }],
    take: MAX_SCHEDULE_BATCH * 10,
    select: {
      id: true,
      status: true,
      channelId: true,
      mediaAssetId: true,
      retryCount: true,
      maxRetries: true,
      nextRunAt: true,
      priority: true,
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
          createdAt: true,
          collectionEpisode: {
            select: {
              collection: {
                select: {
                  extConfig: true,
                },
              },
            },
          },
        },
      },
    },
  });
}

async function backfillNormalDispatchOrderMetadata(channelId: bigint) {
  const normalAssets = await prisma.mediaAsset.findMany({
    where: { channelId },
    select: {
      id: true,
      createdAt: true,
      sourceMeta: true,
    },
    orderBy: [{ createdAt: 'asc' }, { id: 'asc' }],
  });

  const currentOrderNos = normalAssets
    .map((asset) => resolveOrderMeta({ channelId, sourceMeta: asset.sourceMeta }))
    .filter((meta) => meta.orderType === 'normal' && meta.orderNo !== null)
    .map((meta) => meta.orderNo ?? 0);

  let nextOrderNo = currentOrderNos.length > 0 ? Math.max(...currentOrderNos) + 1 : 1;
  let updatedCount = 0;

  for (const asset of normalAssets) {
    const sourceMeta = asObject(asset.sourceMeta);
    const resolved = resolveOrderMeta({ channelId, sourceMeta: asset.sourceMeta });
    if (resolved.orderType !== 'normal') {
      continue;
    }

    const targetOrderNo = resolved.orderNo ?? nextOrderNo;
    const targetMeta = buildNormalOrderMeta({
      channelId,
      orderNo: targetOrderNo,
    });
    const shouldUpdate =
      resolved.orderNo === null ||
      sourceMeta.orderType !== targetMeta.orderType ||
      sourceMeta.orderGroup !== targetMeta.orderGroup ||
      sourceMeta.orderNo !== targetMeta.orderNo ||
      sourceMeta.orderParseFailed !== targetMeta.orderParseFailed;

    if (!shouldUpdate) {
      continue;
    }

    await prisma.mediaAsset.update({
      where: { id: asset.id },
      data: {
        sourceMeta: {
          ...sourceMeta,
          ...targetMeta,
          orderBackfilled: true,
          orderBackfilledAt: new Date().toISOString(),
          orderBackfillBasis: 'createdAt_id',
        },
      },
    });

    if (resolved.orderNo === null) {
      nextOrderNo += 1;
    }
    updatedCount += 1;
  }

  if (updatedCount > 0) {
    logger.info('[dispatch-scheduler] 已回填普通视频派发顺序元数据', {
      channelId: channelId.toString(),
      updatedCount,
      orderType: 'normal',
    });
  }
}

async function backfillCollectionDispatchOrderMetadata(channelId: bigint, collectionName: string) {
  const collectionAssets = await prisma.mediaAsset.findMany({
    where: {
      channelId,
      sourceMeta: {
        path: ['collectionName'],
        equals: collectionName,
      },
    },
    select: {
      id: true,
      sourceMeta: true,
    },
  });

  let updatedCount = 0;

  for (const asset of collectionAssets) {
    const sourceMeta = asObject(asset.sourceMeta);
    const resolved = resolveOrderMeta({ channelId, sourceMeta: asset.sourceMeta });
    if (resolved.orderType !== 'collection' || !resolved.collectionName) {
      continue;
    }

    const targetMeta = buildCollectionOrderMeta({
      channelId,
      collectionName: resolved.collectionName,
      episodeNo: resolved.episodeNo,
      episodeParseFailed: resolved.orderParseFailed,
      orderNo: resolved.orderNo,
    });
    const shouldUpdate =
      sourceMeta.orderType !== targetMeta.orderType ||
      sourceMeta.orderGroup !== targetMeta.orderGroup ||
      sourceMeta.orderNo !== targetMeta.orderNo ||
      sourceMeta.orderParseFailed !== targetMeta.orderParseFailed;

    if (!shouldUpdate) {
      continue;
    }

    await prisma.mediaAsset.update({
      where: { id: asset.id },
      data: {
        sourceMeta: {
          ...sourceMeta,
          ...targetMeta,
          orderBackfilled: true,
          orderBackfilledAt: new Date().toISOString(),
          orderBackfillBasis: 'collection_episode_no',
        },
      },
    });
    updatedCount += 1;
  }

  if (updatedCount > 0) {
    logger.info('[dispatch-scheduler] 已回填合集派发顺序元数据', {
      channelId: channelId.toString(),
      collectionName,
      updatedCount,
      orderType: 'collection',
    });
  }
}

async function repairCollectionOrderMeta(args: {
  mediaAssetId: bigint;
  channelId: bigint;
  sourceMeta: unknown;
  originalName: string;
  navReplyMarkup: unknown;
}) {
  const sourceMeta = asObject(args.sourceMeta);
  const resolved = resolveOrderMeta({ channelId: args.channelId, sourceMeta: args.sourceMeta });
  if (resolved.orderType !== 'collection' || !resolved.collectionName) {
    return false;
  }

  const orderConfig = parseOrderSchedulerConfig(args.navReplyMarkup);
  const parsed = parseEpisodeOrderFromText(args.originalName, orderConfig);
  if (parsed.orderParseFailed || parsed.orderNo === null) {
    return false;
  }

  const targetMeta = buildCollectionOrderMeta({
    channelId: args.channelId,
    collectionName: resolved.collectionName,
    episodeNo: parsed.episodeNo,
    episodeParseFailed: false,
    orderNo: parsed.orderNo,
  });

  await prisma.mediaAsset.update({
    where: { id: args.mediaAssetId },
    data: {
      sourceMeta: {
        ...sourceMeta,
        episodeNo: parsed.episodeNo,
        episodeParseFailed: false,
        episodeMatchedBy: parsed.matchedBy,
        episodeMatchedToken: parsed.matchedToken,
        ...targetMeta,
      },
    },
  });

  logger.info('[dispatch-scheduler] 合集集号已从文件名自动修复', {
    mediaAssetId: args.mediaAssetId.toString(),
    channelId: args.channelId.toString(),
    collectionName: resolved.collectionName,
    episodeNo: parsed.episodeNo,
    orderNo: parsed.orderNo,
    matchedBy: parsed.matchedBy,
    matchedToken: parsed.matchedToken,
    originalName: args.originalName,
  });
  return true;
}

async function canDispatchByOrderGroup(args: {
  channelId: bigint;
  orderGroup: string;
  orderNo: number;
  now: Date;
  collectionGapPolicy?: 'strict' | 'allow_gap';
  collectionAllowedGapSize?: number;
}) {
  const groupAssets = await prisma.mediaAsset.findMany({
    where: {
      channelId: args.channelId,
      sourceMeta: {
        path: ['orderGroup'],
        equals: args.orderGroup,
      },
    },
    select: {
      id: true,
      sourceMeta: true,
      status: true,
      updatedAt: true,
    },
  });

  const prevAssets = groupAssets
    .map((asset) => ({
      id: asset.id,
      status: asset.status,
      updatedAt: asset.updatedAt,
      sourceMeta: asset.sourceMeta,
      orderMeta: resolveOrderMeta({ channelId: args.channelId, sourceMeta: asset.sourceMeta }),
    }))
    .filter((asset) => asset.orderMeta.orderGroup === args.orderGroup && asset.orderMeta.orderNo !== null)
    .filter((asset) => (asset.orderMeta.orderNo ?? 0) < args.orderNo);

  if (prevAssets.length === 0) {
    return { allowed: true as const, blockedByOrderNo: null as number | null };
  }

  const prevAssetIds = prevAssets.map((item) => item.id);

  const successRows = await prisma.dispatchTask.findMany({
    where: {
      mediaAssetId: { in: prevAssetIds },
      status: TaskStatus.success,
    },
    select: { mediaAssetId: true },
    distinct: ['mediaAssetId'],
  });

  const successSet = new Set(successRows.map((row) => row.mediaAssetId.toString()));

  for (const prev of prevAssets) {
    if (successSet.has(prev.id.toString())) continue;

    if (prev.orderMeta.orderType !== 'collection') {
      continue;
    }

    const isFailedOrDeleted = prev.status === MediaStatus.failed || prev.status === MediaStatus.deleted;
    if (!isFailedOrDeleted) continue;

    const elapsedMs = args.now.getTime() - prev.updatedAt.getTime();
    if (Number.isFinite(elapsedMs) && elapsedMs >= COLLECTION_SKIP_GRACE_MS) {
      await prisma.mediaAsset.update({
        where: { id: prev.id },
        data: {
          sourceMeta: {
            ...asObject(prev.sourceMeta),
            skipStatus: 'skipped_missing',
            skipReason: COLLECTION_SKIP_REASON,
            skipAt: args.now.toISOString(),
          },
        },
      });
    }
  }

  const refreshedAssets = await prisma.mediaAsset.findMany({
    where: { id: { in: prevAssetIds } },
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

  const blocked = prevAssets
    .filter((item) => !successSet.has(item.id.toString()) && !skippedSet.has(item.id.toString()))
    .sort((a, b) => (a.orderMeta.orderNo ?? 0) - (b.orderMeta.orderNo ?? 0))[0];

  const remainingBlockedAssets = prevAssets
    .filter((item) => !successSet.has(item.id.toString()) && !skippedSet.has(item.id.toString()))
    .sort((a, b) => (a.orderMeta.orderNo ?? 0) - (b.orderMeta.orderNo ?? 0));

  if (
    args.collectionGapPolicy === 'allow_gap' &&
    Math.max(0, Math.floor(args.collectionAllowedGapSize ?? 0)) > 0 &&
    remainingBlockedAssets.length > 0 &&
    remainingBlockedAssets.length <= Math.max(0, Math.floor(args.collectionAllowedGapSize ?? 0)) &&
    remainingBlockedAssets.every(
      (item) => item.orderMeta.orderType === 'collection' && (item.status === MediaStatus.failed || item.status === MediaStatus.deleted),
    )
  ) {
    return { allowed: true as const, blockedByOrderNo: null as number | null };
  }

  if (!blocked) {
    return { allowed: true as const, blockedByOrderNo: null as number | null };
  }

  return {
    allowed: false as const,
    blockedByOrderNo: blocked.orderMeta.orderNo ?? null,
  };
}

export async function scheduleDueDispatchTasks() {
  const now = new Date();
  let dueTasks = await loadDueDispatchTasks(now);
  const normalChannelsToBackfill = new Set<string>();
  const collectionPairsToBackfill = new Map<string, { channelId: bigint; collectionName: string }>();
  let shouldReloadDueTasks = false;

  for (const task of dueTasks) {
    const resolved = resolveOrderMeta({ channelId: task.channelId, sourceMeta: task.mediaAsset.sourceMeta });
    const sourceMeta = asObject(task.mediaAsset.sourceMeta);

    if (resolved.orderType === 'collection' && (resolved.orderParseFailed || resolved.orderNo === null)) {
      const repaired = await repairCollectionOrderMeta({
        mediaAssetId: task.mediaAssetId,
        channelId: task.channelId,
        sourceMeta: task.mediaAsset.sourceMeta,
        originalName: task.mediaAsset.originalName || '',
        navReplyMarkup: task.channel.navReplyMarkup,
      });
      if (repaired) {
        shouldReloadDueTasks = true;
        continue;
      }
    }

    if (resolved.orderType === 'normal' && resolved.orderNo === null) {
      normalChannelsToBackfill.add(task.channelId.toString());
      continue;
    }

    if (
      resolved.orderType === 'collection' &&
      resolved.collectionName &&
      (sourceMeta.orderType !== 'collection' ||
        typeof sourceMeta.orderGroup !== 'string' ||
        sourceMeta.orderNo !== resolved.orderNo ||
        typeof sourceMeta.orderParseFailed !== 'boolean')
    ) {
      collectionPairsToBackfill.set(`${task.channelId.toString()}:${resolved.collectionName}`, {
        channelId: task.channelId,
        collectionName: resolved.collectionName,
      });
    }
  }

  for (const channelIdStr of normalChannelsToBackfill) {
    await backfillNormalDispatchOrderMetadata(BigInt(channelIdStr));
    shouldReloadDueTasks = true;
  }

  for (const pair of collectionPairsToBackfill.values()) {
    await backfillCollectionDispatchOrderMetadata(pair.channelId, pair.collectionName);
    shouldReloadDueTasks = true;
  }

  if (shouldReloadDueTasks) {
    dueTasks = await loadDueDispatchTasks(now);
  }

  dueTasks.sort((left, right) => {
    if (left.priority !== right.priority) {
      return left.priority - right.priority;
    }
    if (left.nextRunAt.getTime() !== right.nextRunAt.getTime()) {
      return left.nextRunAt.getTime() - right.nextRunAt.getTime();
    }
    if (left.channelId !== right.channelId) {
      return left.channelId < right.channelId ? -1 : 1;
    }

    const leftOrder = resolveOrderMeta({ channelId: left.channelId, sourceMeta: left.mediaAsset.sourceMeta });
    const rightOrder = resolveOrderMeta({ channelId: right.channelId, sourceMeta: right.mediaAsset.sourceMeta });
    const leftOrderNo = leftOrder.orderNo ?? Number.MAX_SAFE_INTEGER;
    const rightOrderNo = rightOrder.orderNo ?? Number.MAX_SAFE_INTEGER;
    if (leftOrderNo !== rightOrderNo) {
      return leftOrderNo - rightOrderNo;
    }
    if (leftOrder.orderGroup !== rightOrder.orderGroup) {
      return leftOrder.orderGroup.localeCompare(rightOrder.orderGroup, 'zh-CN');
    }
    if (left.mediaAsset.createdAt.getTime() !== right.mediaAsset.createdAt.getTime()) {
      return left.mediaAsset.createdAt.getTime() - right.mediaAsset.createdAt.getTime();
    }
    return left.id < right.id ? -1 : 1;
  });

  const queuedChannelIds = new Set<string>();
  let queuedCount = 0;

  for (const task of dueTasks.slice(0, MAX_SCHEDULE_BATCH * 10)) {
    const channelIdStr = task.channelId.toString();

    if (queuedChannelIds.has(channelIdStr)) {
      continue;
    }

    const orderMeta = resolveOrderMeta({ channelId: task.channelId, sourceMeta: task.mediaAsset.sourceMeta });
    const orderCfg = parseOrderSchedulerConfig(task.channel.navReplyMarkup);
    const collectionDispatchOrderCfg =
      orderMeta.orderType === 'collection'
        ? resolveCollectionDispatchOrderConfig({
            channelConfig: orderCfg,
            extConfig: task.mediaAsset.collectionEpisode?.collection?.extConfig,
          })
        : null;
    const shouldApplyOrderGate =
      orderMeta.orderType === 'collection'
        ? Boolean(collectionDispatchOrderCfg?.orderGateEnabled)
        : orderCfg.orderGateEnabled && orderCfg.normalOrderDispatchGateEnabled;

    if (orderMeta.orderParseFailed || orderMeta.orderNo === null) {
      await prisma.dispatchTask.update({
        where: { id: task.id },
        data: {
          status: TaskStatus.scheduled,
          nextRunAt: new Date(Date.now() + 10 * 60 * 1000),
        },
      });

      logger.warn('[dispatch-scheduler] 顺序元数据不可用，阻塞分发等待回填或重扫', {
        taskId: task.id.toString(),
        channelId: channelIdStr,
        mediaAssetId: task.mediaAssetId.toString(),
        orderType: orderMeta.orderType,
        orderGroup: orderMeta.orderGroup,
        orderNo: orderMeta.orderNo,
      });
      continue;
    }

    if (shouldApplyOrderGate) {
      const gateResult = await canDispatchByOrderGroup({
        channelId: task.channelId,
        orderGroup: orderMeta.orderGroup,
        orderNo: orderMeta.orderNo,
        now,
        collectionGapPolicy: collectionDispatchOrderCfg?.collectionGapPolicy,
        collectionAllowedGapSize: collectionDispatchOrderCfg?.collectionAllowedGapSize,
      });

      if (!gateResult.allowed) {
        const bypassReady =
          Boolean(collectionDispatchOrderCfg?.orderHeadBypassEnabled ?? orderCfg.orderHeadBypassEnabled) &&
          task.status === TaskStatus.failed &&
          task.retryCount >= DISPATCH_HEAD_BYPASS_RETRY_THRESHOLD &&
          task.retryCount < task.maxRetries;

        if (bypassReady) {
          const bypassMinutes = collectionDispatchOrderCfg?.orderHeadBypassMinutes ?? orderCfg.orderHeadBypassMinutes;
          const bypassNextRunAt = new Date(Date.now() + bypassMinutes * 60 * 1000);
          await prisma.dispatchTask.update({
            where: { id: task.id },
            data: {
              status: TaskStatus.failed,
              nextRunAt: bypassNextRunAt,
            },
          });

          await prisma.dispatchTaskLog.create({
            data: {
              dispatchTaskId: task.id,
              action: 'dispatch_order_head_bypass_applied',
              detail: {
                channelId: channelIdStr,
                mediaAssetId: task.mediaAssetId.toString(),
                orderType: orderMeta.orderType,
                orderGroup: orderMeta.orderGroup,
                orderNo: orderMeta.orderNo,
                blockedByOrderNo: gateResult.blockedByOrderNo,
                bypassApplied: true,
                reason: 'head_block_retry_threshold',
                orderHeadBypassMinutes: bypassMinutes,
                retryCount: task.retryCount,
                maxRetries: task.maxRetries,
                bypassNextRunAt: bypassNextRunAt.toISOString(),
              },
            },
          });

          logger.warn('[dispatch-scheduler] 顺序头阻塞旁路生效，延后重试', {
            taskId: task.id.toString(),
            channelId: channelIdStr,
            mediaAssetId: task.mediaAssetId.toString(),
            orderType: orderMeta.orderType,
            orderGroup: orderMeta.orderGroup,
            orderNo: orderMeta.orderNo,
            blockedByOrderNo: gateResult.blockedByOrderNo,
            orderHeadBypassMinutes: bypassMinutes,
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

        await prisma.dispatchTaskLog.create({
          data: {
            dispatchTaskId: task.id,
            action: 'dispatch_order_gate_blocked',
            detail: {
              channelId: channelIdStr,
              mediaAssetId: task.mediaAssetId.toString(),
              orderType: orderMeta.orderType,
              orderGroup: orderMeta.orderGroup,
              orderNo: orderMeta.orderNo,
              blockedByOrderNo: gateResult.blockedByOrderNo,
              bypassApplied: false,
              reason: 'waiting_for_previous_order_success',
            },
          },
        });

        logger.info('[dispatch-scheduler] 顺序闸门阻塞当前分发任务', {
          taskId: task.id.toString(),
          channelId: channelIdStr,
          mediaAssetId: task.mediaAssetId.toString(),
          orderType: orderMeta.orderType,
          orderGroup: orderMeta.orderGroup,
          orderNo: orderMeta.orderNo,
          blockedByOrderNo: gateResult.blockedByOrderNo,
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

      await prisma.dispatchTaskLog.create({
        data: {
          dispatchTaskId: task.id,
          action: 'dispatch_task_enqueued',
          detail: {
            channelId: channelIdStr,
            mediaAssetId: task.mediaAssetId.toString(),
            orderType: orderMeta.orderType,
            orderGroup: orderMeta.orderGroup,
            orderNo: orderMeta.orderNo,
          },
        },
      });

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
      orderBy: [{ channelId: 'asc' }, { createdAt: 'asc' }, { id: 'asc' }],
      select: {
        id: true,
        channelId: true,
        createdAt: true,
        sourceMeta: true,
      },
      take: 200,
    });

    const normalChannelsToBackfill = new Set<string>();
    const collectionPairsToBackfill = new Map<string, { channelId: bigint; collectionName: string }>();

    for (const asset of unscheduledAssets) {
      const resolved = resolveOrderMeta({ channelId: asset.channelId, sourceMeta: asset.sourceMeta });
      const sourceMeta = asObject(asset.sourceMeta);

      if (resolved.orderType === 'normal' && resolved.orderNo === null) {
        normalChannelsToBackfill.add(asset.channelId.toString());
        continue;
      }

      if (
        resolved.orderType === 'collection' &&
        resolved.collectionName &&
        (sourceMeta.orderType !== 'collection' ||
          typeof sourceMeta.orderGroup !== 'string' ||
          sourceMeta.orderNo !== resolved.orderNo ||
          typeof sourceMeta.orderParseFailed !== 'boolean')
      ) {
        collectionPairsToBackfill.set(`${asset.channelId.toString()}:${resolved.collectionName}`, {
          channelId: asset.channelId,
          collectionName: resolved.collectionName,
        });
      }
    }

    for (const channelIdStr of normalChannelsToBackfill) {
      await backfillNormalDispatchOrderMetadata(BigInt(channelIdStr));
    }

    for (const pair of collectionPairsToBackfill.values()) {
      await backfillCollectionDispatchOrderMetadata(pair.channelId, pair.collectionName);
    }

    const orderedAssets = (
      normalChannelsToBackfill.size > 0 || collectionPairsToBackfill.size > 0
        ? await prisma.mediaAsset.findMany({
            where: {
              status: MediaStatus.relay_uploaded,
              telegramFileId: { not: null },
              dispatchTasks: {
                none: {},
              },
            },
            orderBy: [{ channelId: 'asc' }, { createdAt: 'asc' }, { id: 'asc' }],
            select: {
              id: true,
              channelId: true,
              createdAt: true,
              sourceMeta: true,
            },
            take: 200,
          })
        : unscheduledAssets
    ).sort((left, right) => {
      if (left.channelId !== right.channelId) {
        return left.channelId < right.channelId ? -1 : 1;
      }

      const leftOrder = resolveOrderMeta({ channelId: left.channelId, sourceMeta: left.sourceMeta });
      const rightOrder = resolveOrderMeta({ channelId: right.channelId, sourceMeta: right.sourceMeta });
      const leftOrderNo = leftOrder.orderNo ?? Number.MAX_SAFE_INTEGER;
      const rightOrderNo = rightOrder.orderNo ?? Number.MAX_SAFE_INTEGER;
      if (leftOrderNo !== rightOrderNo) {
        return leftOrderNo - rightOrderNo;
      }
      if (leftOrder.orderGroup !== rightOrder.orderGroup) {
        return leftOrder.orderGroup.localeCompare(rightOrder.orderGroup, 'zh-CN');
      }
      if (left.createdAt.getTime() !== right.createdAt.getTime()) {
        return left.createdAt.getTime() - right.createdAt.getTime();
      }
      return left.id < right.id ? -1 : 1;
    });

    let createdCount = 0;
    const now = new Date();

    for (const asset of orderedAssets) {
      const scheduleAt = new Date(now.getTime() + createdCount);
      await prisma.dispatchTask.create({
        data: {
          channelId: asset.channelId,
          mediaAssetId: asset.id,
          status: TaskStatus.pending,
          scheduleSlot: scheduleAt,
          plannedAt: scheduleAt,
          nextRunAt: scheduleAt,
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

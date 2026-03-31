import { MediaStatus } from '@prisma/client';
import {
  MAX_SCHEDULE_BATCH,
  TYPEA_INGEST_LEASE_MS,
  TYPEA_INGEST_MAX_RETRIES,
  TYPEA_INGEST_STALE_MS,
} from '../config/env';
import { prisma } from '../infra/prisma';
import { relayUploadQueue } from '../infra/redis';
import { logger } from '../logger';
import { updateTaskDefinitionRunStatus } from '../services/task-definition.service';
import { enqueueRelayAssetsFromTaskDefinition } from '../services/relay.service';
import { TYPEA_INGEST_FINAL_REASON } from '../shared/metrics';
import {
  buildCollectionOrderMeta,
  buildNormalOrderMeta,
  parseOrderSchedulerConfig,
  resolveOrderMeta,
} from '../shared/order-meta';

function asObject(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, unknown>;
}

async function loadRelayCandidateAssets(staleIngestingBefore: Date) {
  return prisma.mediaAsset.findMany({
    where: {
      telegramFileId: null,
      relayMessageId: null,
      OR: [
        { status: MediaStatus.ready },
        {
          status: MediaStatus.ingesting,
          updatedAt: { lte: staleIngestingBefore },
        },
      ],
    },
    orderBy: [{ channelId: 'asc' }, { createdAt: 'asc' }, { id: 'asc' }],
    take: MAX_SCHEDULE_BATCH * 10,
    select: {
      id: true,
      channelId: true,
      originalName: true,
      status: true,
      sourceMeta: true,
      updatedAt: true,
      createdAt: true,
      channel: {
        select: {
          navReplyMarkup: true,
        },
      },
    },
  });
}

async function backfillNormalOrderMetadata(channelId: bigint) {
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
    const metaObj = asObject(asset.sourceMeta);
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
      metaObj.orderType !== targetMeta.orderType ||
      metaObj.orderGroup !== targetMeta.orderGroup ||
      metaObj.orderNo !== targetMeta.orderNo ||
      metaObj.orderParseFailed !== targetMeta.orderParseFailed;

    if (!shouldUpdate) {
      continue;
    }

    await prisma.mediaAsset.update({
      where: { id: asset.id },
      data: {
        sourceMeta: {
          ...metaObj,
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
    logger.info('[relay-scheduler] 已回填普通视频顺序元数据', {
      channelId: channelId.toString(),
      updatedCount,
      orderType: 'normal',
    });
  }
}

async function backfillCollectionOrderMetadata(channelId: bigint, collectionName: string) {
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
    const metaObj = asObject(asset.sourceMeta);
    const resolved = resolveOrderMeta({ channelId, sourceMeta: asset.sourceMeta });
    if (resolved.orderType !== 'collection' || !resolved.collectionName) {
      continue;
    }

    const targetMeta = buildCollectionOrderMeta({
      channelId,
      collectionName: resolved.collectionName,
      episodeNo: resolved.episodeNo,
      episodeParseFailed: resolved.orderParseFailed,
    });
    const shouldUpdate =
      metaObj.orderType !== targetMeta.orderType ||
      metaObj.orderGroup !== targetMeta.orderGroup ||
      metaObj.orderNo !== targetMeta.orderNo ||
      metaObj.orderParseFailed !== targetMeta.orderParseFailed;

    if (!shouldUpdate) {
      continue;
    }

    await prisma.mediaAsset.update({
      where: { id: asset.id },
      data: {
        sourceMeta: {
          ...metaObj,
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
    logger.info('[relay-scheduler] 已回填合集顺序元数据', {
      channelId: channelId.toString(),
      collectionName,
      updatedCount,
      orderType: 'collection',
    });
  }
}

async function canUploadByOrderGroup(args: {
  channelId: bigint;
  orderGroup: string;
  orderNo: number;
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
      status: true,
      sourceMeta: true,
      telegramFileId: true,
      relayMessageId: true,
    },
  });

  const blockedAsset = groupAssets
    .map((asset) => ({
      id: asset.id,
      status: asset.status,
      telegramFileId: asset.telegramFileId,
      relayMessageId: asset.relayMessageId,
      resolved: resolveOrderMeta({ channelId: args.channelId, sourceMeta: asset.sourceMeta }),
    }))
    .filter((asset) => asset.resolved.orderGroup === args.orderGroup && asset.resolved.orderNo !== null)
    .filter((asset) => (asset.resolved.orderNo ?? 0) < args.orderNo)
    .filter((asset) => {
      const uploaded =
        asset.status === MediaStatus.relay_uploaded ||
        Boolean(asset.telegramFileId) ||
        Boolean(asset.relayMessageId);
      return !uploaded;
    })
    .sort((left, right) => (left.resolved.orderNo ?? 0) - (right.resolved.orderNo ?? 0))[0];

  if (!blockedAsset) {
    return { allowed: true as const, blockedByOrderNo: null as number | null };
  }

  return {
    allowed: false as const,
    blockedByOrderNo: blockedAsset.resolved.orderNo ?? null,
  };
}

export async function scheduleDueRelayUploadTasks() {
  const staleIngestingBefore = new Date(Date.now() - TYPEA_INGEST_STALE_MS);
  let candidateAssets = await loadRelayCandidateAssets(staleIngestingBefore);

  const normalChannelsToBackfill = new Set<string>();
  const collectionPairsToBackfill = new Map<string, { channelId: bigint; collectionName: string }>();

  for (const asset of candidateAssets) {
    const resolved = resolveOrderMeta({ channelId: asset.channelId, sourceMeta: asset.sourceMeta });
    const metaObj = asObject(asset.sourceMeta);

    if (resolved.orderType === 'normal' && resolved.orderNo === null) {
      normalChannelsToBackfill.add(asset.channelId.toString());
      continue;
    }

    if (
      resolved.orderType === 'collection' &&
      resolved.collectionName &&
      (metaObj.orderType !== 'collection' ||
        typeof metaObj.orderGroup !== 'string' ||
        metaObj.orderNo !== resolved.orderNo ||
        typeof metaObj.orderParseFailed !== 'boolean')
    ) {
      collectionPairsToBackfill.set(`${asset.channelId.toString()}:${resolved.collectionName}`, {
        channelId: asset.channelId,
        collectionName: resolved.collectionName,
      });
    }
  }

  for (const channelIdStr of normalChannelsToBackfill) {
    await backfillNormalOrderMetadata(BigInt(channelIdStr));
  }

  for (const pair of collectionPairsToBackfill.values()) {
    await backfillCollectionOrderMetadata(pair.channelId, pair.collectionName);
  }

  if (normalChannelsToBackfill.size > 0 || collectionPairsToBackfill.size > 0) {
    candidateAssets = await loadRelayCandidateAssets(staleIngestingBefore);
  }

  const groupedByChannel = new Map<string, typeof candidateAssets>();
  for (const asset of candidateAssets) {
    const key = asset.channelId.toString();
    const bucket = groupedByChannel.get(key);
    if (bucket) {
      bucket.push(asset);
    } else {
      groupedByChannel.set(key, [asset]);
    }
  }

  for (const bucket of groupedByChannel.values()) {
    bucket.sort((left, right) => {
      const leftMeta = resolveOrderMeta({ channelId: left.channelId, sourceMeta: left.sourceMeta });
      const rightMeta = resolveOrderMeta({ channelId: right.channelId, sourceMeta: right.sourceMeta });
      if (leftMeta.orderGroup !== rightMeta.orderGroup) {
        return leftMeta.orderGroup.localeCompare(rightMeta.orderGroup, 'zh-CN');
      }
      const leftOrderNo = leftMeta.orderNo ?? Number.MAX_SAFE_INTEGER;
      const rightOrderNo = rightMeta.orderNo ?? Number.MAX_SAFE_INTEGER;
      if (leftOrderNo !== rightOrderNo) {
        return leftOrderNo - rightOrderNo;
      }
      if (left.createdAt.getTime() !== right.createdAt.getTime()) {
        return left.createdAt.getTime() - right.createdAt.getTime();
      }
      return Number(left.id - right.id);
    });
  }

  const selectedAssets: typeof candidateAssets = [];
  while (selectedAssets.length < MAX_SCHEDULE_BATCH && groupedByChannel.size > 0) {
    for (const [key, bucket] of groupedByChannel) {
      const next = bucket.shift();
      if (next) {
        selectedAssets.push(next);
      }
      if (bucket.length === 0) {
        groupedByChannel.delete(key);
      }
      if (selectedAssets.length >= MAX_SCHEDULE_BATCH) {
        break;
      }
    }
  }

  let queuedCount = 0;
  let staleRecoveredCount = 0;
  let failedFinalCount = 0;

  for (const asset of selectedAssets) {
    const sourceMeta = asObject(asset.sourceMeta);
    const relayChannelId = sourceMeta.relayChannelId;
    if (typeof relayChannelId !== 'string' || !relayChannelId.trim()) continue;

    const orderMeta = resolveOrderMeta({ channelId: asset.channelId, sourceMeta: asset.sourceMeta });
    const orderCfg = parseOrderSchedulerConfig(asset.channel.navReplyMarkup);
    const shouldApplyOrderGate =
      orderCfg.orderGateEnabled &&
      (orderMeta.orderType === 'collection' || orderCfg.normalOrderUploadGateEnabled);

    const whereReady = {
      id: asset.id,
      status: MediaStatus.ready,
      telegramFileId: null,
      relayMessageId: null,
    };

    const whereStaleIngesting = {
      id: asset.id,
      status: MediaStatus.ingesting,
      telegramFileId: null,
      relayMessageId: null,
      updatedAt: { lte: staleIngestingBefore },
    };

    if (orderMeta.orderParseFailed || orderMeta.orderNo === null) {
      await prisma.mediaAsset.updateMany({
        where: asset.status === MediaStatus.ready ? whereReady : whereStaleIngesting,
        data: {
          status: MediaStatus.ready,
          sourceMeta: {
            ...sourceMeta,
            ingestLeaseUntil: null,
            ingestWorkerJobId: null,
            orderLastBlockedAt: new Date().toISOString(),
          },
        },
      });

      logger.warn('[relay-scheduler] 顺序元数据不可用，阻塞上传任务', {
        mediaAssetId: asset.id.toString(),
        channelId: asset.channelId.toString(),
        orderType: orderMeta.orderType,
        orderGroup: orderMeta.orderGroup,
        orderNo: orderMeta.orderNo,
        blockedByOrderNo: null,
      });
      continue;
    }

    if (shouldApplyOrderGate) {
      const gateResult = await canUploadByOrderGroup({
        channelId: asset.channelId,
        orderGroup: orderMeta.orderGroup,
        orderNo: orderMeta.orderNo,
      });

      if (!gateResult.allowed) {
        await prisma.mediaAsset.updateMany({
          where: asset.status === MediaStatus.ready ? whereReady : whereStaleIngesting,
          data: {
            status: MediaStatus.ready,
            sourceMeta: {
              ...sourceMeta,
              ingestLeaseUntil: null,
              ingestWorkerJobId: null,
              orderLastBlockedAt: new Date().toISOString(),
              blockedByOrderNo: gateResult.blockedByOrderNo,
            },
          },
        });

        logger.info('[relay-scheduler] 上传顺序闸门阻塞当前资源', {
          mediaAssetId: asset.id.toString(),
          channelId: asset.channelId.toString(),
          orderType: orderMeta.orderType,
          orderGroup: orderMeta.orderGroup,
          orderNo: orderMeta.orderNo,
          blockedByOrderNo: gateResult.blockedByOrderNo,
        });
        continue;
      }
    }

    const ingestRetryCountRaw = sourceMeta.ingestRetryCount;
    const ingestRetryCount =
      typeof ingestRetryCountRaw === 'number'
        ? ingestRetryCountRaw
        : typeof ingestRetryCountRaw === 'string' && /^\d+$/.test(ingestRetryCountRaw)
          ? Number(ingestRetryCountRaw)
          : 0;

    if (asset.status === MediaStatus.ingesting) {
      if (ingestRetryCount >= TYPEA_INGEST_MAX_RETRIES) {
        await prisma.mediaAsset.update({
          where: { id: asset.id },
          data: {
            status: MediaStatus.failed,
            ingestError: `ingesting stale exceed max retries (${TYPEA_INGEST_MAX_RETRIES})`,
            sourceMeta: {
              ...sourceMeta,
              ingestRetryCount,
              ingestFinalReason: TYPEA_INGEST_FINAL_REASON.staleIngestingExceeded,
            },
          },
        });
        logger.warn('[relay-scheduler] ingesting 超时重试达到上限', {
          mediaAssetId: asset.id.toString(),
          channelId: asset.channelId.toString(),
          orderType: orderMeta.orderType,
          orderGroup: orderMeta.orderGroup,
          orderNo: orderMeta.orderNo,
        });
        failedFinalCount += 1;
        continue;
      }

      staleRecoveredCount += 1;
    }

    const updated = await prisma.mediaAsset.updateMany({
      where: asset.status === MediaStatus.ready ? whereReady : whereStaleIngesting,
      data: {
        status: MediaStatus.ingesting,
        updatedAt: new Date(),
        sourceMeta: {
          ...sourceMeta,
          ingestRetryCount: asset.status === MediaStatus.ingesting ? ingestRetryCount + 1 : ingestRetryCount,
          ingestLastScheduledAt: new Date().toISOString(),
          ingestLeaseUntil: new Date(Date.now() + TYPEA_INGEST_LEASE_MS).toISOString(),
          ingestLastHeartbeatAt: new Date().toISOString(),
          ingestWorkerJobId: null,
        },
      },
    });

    if (updated.count === 0) continue;

    const jobId = `relay-upload-${asset.id.toString()}`;
    const existingJob = await relayUploadQueue.getJob(jobId);
    if (existingJob) {
      const state = await existingJob.getState();
      if (state === 'failed') {
        await existingJob.remove();
      } else {
        continue;
      }
    }

    await relayUploadQueue.add(
      'relay-upload',
      {
        mediaAssetId: asset.id.toString(),
        relayChannelId,
      },
      {
        jobId,
        removeOnComplete: true,
        removeOnFail: 200,
      },
    );

    logger.info('[relay-scheduler] 已放行上传任务', {
      mediaAssetId: asset.id.toString(),
      channelId: asset.channelId.toString(),
      orderType: orderMeta.orderType,
      orderGroup: orderMeta.orderGroup,
      orderNo: orderMeta.orderNo,
    });
    queuedCount += 1;
  }

  logger.info('[typea_metrics] relay schedule tick', {
    typea_enqueue_total: queuedCount,
    typea_ingesting_stale_total: staleRecoveredCount,
    typea_failed_final_total: failedFinalCount,
    task_run_total: queuedCount,
    task_failed_total: 0,
    task_dead_total: failedFinalCount,
    metric_labels: {
      typea_enqueue_total: 'TypeA 入队总数',
      typea_ingesting_stale_total: 'TypeA ingesting 超时回收总数',
      typea_failed_final_total: 'TypeA 失败终态总数',
    },
    mode: 'round_robin_by_channel_with_order_gate',
  });
}

export async function scheduleRelayForDefinition(taskDefinitionId: bigint) {
  try {
    const enqueueSummary = await enqueueRelayAssetsFromTaskDefinition(taskDefinitionId);

    logger.info('[typea_metrics] relay scan summary', {
      typea_scan_files_total: enqueueSummary.scannedFiles,
      typea_enqueue_total: enqueueSummary.enqueuedTasks,
      typea_rejected_too_large_total: enqueueSummary.rejectedTooLarge ?? 0,
      metric_labels: {
        typea_scan_files_total: 'TypeA 扫描文件总数',
        typea_enqueue_total: 'TypeA 入队总数（扫描阶段）',
        typea_rejected_too_large_total: 'TypeA 超大小文件拒绝总数（扫描阶段）',
      },
      taskDefinitionId: taskDefinitionId.toString(),
    });

    await scheduleDueRelayUploadTasks();
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'success',
      summary: {
        executor: 'relay_upload',
        ...enqueueSummary,
        message: '中转上传扫描与调度完成',
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : '未知错误';
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'failed',
      summary: {
        executor: 'relay_upload',
        error: `中转上传调度失败: ${message}`,
      },
    });

    throw error;
  }
}

/**
 * Relay Scheduler：扫描并入队到期的中转上传任务。
 * 在 bootstrap 定时触发，负责状态修复、分组轮转与投递 relay-upload worker。
 */

import { stat } from 'node:fs/promises';
import { MediaStatus } from '@prisma/client';
import {
  MAX_SCHEDULE_BATCH,
  RELAY_MTIME_COOLDOWN_MS,
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

async function shouldDeferHotStaleIngestingAsset(asset: {
  id: bigint;
  channelId: bigint;
  localPath: string;
}) {
  try {
    const fileStat = await stat(asset.localPath);
    const ageMs = Date.now() - fileStat.mtimeMs;
    if (ageMs < RELAY_MTIME_COOLDOWN_MS) {
      return {
        defer: true,
        reason: 'mtime_cooldown_not_reached',
        ageMs,
      } as const;
    }

    return {
      defer: false,
      ageMs,
    } as const;
  } catch (error) {
    return {
      defer: true,
      reason: 'stat_failed',
      error: error instanceof Error ? error.message : String(error),
    } as const;
  }
}

// 扫描到期中转任务并按分组策略入队上传。
export async function scheduleDueRelayUploadTasks() {
  const staleIngestingBefore = new Date(Date.now() - TYPEA_INGEST_STALE_MS);

  const candidateAssets = await prisma.mediaAsset.findMany({
    where: {
      telegramFileId: null,
      relayMessageId: null, // 🔴 绝对防御：只要拿了流水凭证在等待提取的，绝对不抓取
      OR: [
        { status: MediaStatus.ready },
        {
          status: MediaStatus.ingesting,
          updatedAt: { lte: staleIngestingBefore },
        },
      ],
    },
    orderBy: [{ channelId: 'asc' }, { createdAt: 'asc' }],
    take: MAX_SCHEDULE_BATCH * 10,
    select: {
      id: true,
      channelId: true,
      originalName: true,
      status: true,
      sourceMeta: true,
      localPath: true,
      updatedAt: true,
    },
  });

  const groupedByGroupKey = new Map<string, typeof candidateAssets>();
  const groupSizeByKey = new Map<string, number>();
  for (const asset of candidateAssets) {
    const sourceMeta = (asset.sourceMeta ?? {}) as Record<string, unknown>;
    const groupKeyRaw = sourceMeta.groupKey;
    const groupKey =
      typeof groupKeyRaw === 'string' && groupKeyRaw.trim()
        ? groupKeyRaw.trim()
        : `asset-${asset.id.toString()}`;
    const key = `${asset.channelId.toString()}::${groupKey}`;
    const bucket = groupedByGroupKey.get(key);
    if (bucket) {
      bucket.push(asset);
    } else {
      groupedByGroupKey.set(key, [asset]);
    }
    groupSizeByKey.set(key, (groupSizeByKey.get(key) ?? 0) + 1);
  }

  const selectedAssets: typeof candidateAssets = [];
  while (selectedAssets.length < MAX_SCHEDULE_BATCH && groupedByGroupKey.size > 0) {
    for (const [key, bucket] of groupedByGroupKey) {
      const next = bucket.shift();
      if (next) {
        selectedAssets.push(next);
      }
      if (bucket.length === 0) {
        groupedByGroupKey.delete(key);
      }
      if (selectedAssets.length >= MAX_SCHEDULE_BATCH) {
        break;
      }
    }
  }

  const selectedGroups = new Map<string, typeof selectedAssets>();
  for (const asset of selectedAssets) {
    const sourceMeta = (asset.sourceMeta ?? {}) as Record<string, unknown>;
    const groupKeyRaw = sourceMeta.groupKey;
    const groupKey =
      typeof groupKeyRaw === 'string' && groupKeyRaw.trim()
        ? groupKeyRaw.trim()
        : `asset-${asset.id.toString()}`;
    const key = `${asset.channelId.toString()}::${groupKey}`;
    const list = selectedGroups.get(key);
    if (list) list.push(asset);
    else selectedGroups.set(key, [asset]);
  }

  let queuedCount = 0;
  let groupedQueueJobs = 0;
  let singleQueueJobs = 0;
  let staleRecoveredCount = 0;
  let failedFinalCount = 0;

  for (const [channelGroupKey, groupAssets] of selectedGroups) {
    const head = groupAssets[0];
    if (!head) continue;
    const sourceMeta = (head.sourceMeta ?? {}) as Record<string, unknown>;
    const relayChannelId = sourceMeta.relayChannelId;
    const groupKeyRaw = sourceMeta.groupKey;
    const groupKey =
      typeof groupKeyRaw === 'string' && groupKeyRaw.trim()
        ? groupKeyRaw.trim()
        : `asset-${head.id.toString()}`;

    if (typeof relayChannelId !== 'string' || !relayChannelId.trim()) continue;

    const preparedAssetIds: string[] = [];

    for (const asset of groupAssets) {
      const sourceMeta = (asset.sourceMeta ?? {}) as Record<string, unknown>;
      const ingestRetryCountRaw = sourceMeta.ingestRetryCount;
      const ingestRetryCount =
        typeof ingestRetryCountRaw === 'number'
          ? ingestRetryCountRaw
          : typeof ingestRetryCountRaw === 'string' && /^\d+$/.test(ingestRetryCountRaw)
            ? Number(ingestRetryCountRaw)
            : 0;

      if (asset.status === MediaStatus.ingesting) {
        // 备注：stale ingesting 的重调度不能只看任务心跳；文件若仍在静默冷却期，重投只会再次卡在 worker 的 wait_for_stable。
        const cooldownDecision = await shouldDeferHotStaleIngestingAsset(asset);
        if (cooldownDecision.defer) {
          logger.info('[typea_metrics] stale ingesting deferred by file cooldown', {
            mediaAssetId: asset.id.toString(),
            channelId: asset.channelId.toString(),
            filePath: asset.localPath,
            reason: cooldownDecision.reason,
            ageMs: 'ageMs' in cooldownDecision ? Math.floor(cooldownDecision.ageMs) : null,
            requiredCooldownMs: RELAY_MTIME_COOLDOWN_MS,
            statError: 'error' in cooldownDecision ? cooldownDecision.error : null,
          });
          continue;
        }

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
          failedFinalCount += 1;
          continue;
        }
      }

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

      if (asset.status === MediaStatus.ingesting) {
        staleRecoveredCount += 1;
      }

      preparedAssetIds.push(asset.id.toString());
    }

    if (preparedAssetIds.length === 0) {
      continue;
    }

    const isGroupedBatch = preparedAssetIds.length > 1;
    const groupJobId = isGroupedBatch
      ? `relay-upload-group-${channelGroupKey}`
      : `relay-upload-${preparedAssetIds[0]}`;

    const existingJob = await relayUploadQueue.getJob(groupJobId);
    if (existingJob) {
      const state = await existingJob.getState();
      if (state === 'failed') {
        await existingJob.remove();
      } else {
        continue;
      }
    }

    if (isGroupedBatch) {
      await relayUploadQueue.add(
        'relay-upload-grouped',
        {
          relayChannelId,
          groupKey,
          groupBatchSize: preparedAssetIds.length,
          groupDispatchMode: 'grouped_scan',
          mediaAssetIds: preparedAssetIds,
        },
        {
          jobId: groupJobId,
          removeOnComplete: true,
          removeOnFail: 200,
        },
      );
      queuedCount += preparedAssetIds.length;
      groupedQueueJobs += 1;
    } else {
      await relayUploadQueue.add(
        'relay-upload',
        {
          mediaAssetId: preparedAssetIds[0],
          relayChannelId,
          groupKey,
          groupBatchSize: 1,
          groupDispatchMode: 'single_fallback',
        },
        {
          jobId: groupJobId,
          removeOnComplete: true,
          removeOnFail: 200,
        },
      );
      queuedCount += 1;
      singleQueueJobs += 1;
    }
  }

  logger.info('[typea_metrics] relay schedule tick', {
    typea_enqueue_total: queuedCount,
    typea_ingesting_stale_total: staleRecoveredCount,
    typea_failed_final_total: failedFinalCount,
    typea_group_queue_jobs_total: groupedQueueJobs,
    typea_single_queue_jobs_total: singleQueueJobs,
    task_run_total: queuedCount,
    task_failed_total: 0,
    task_dead_total: failedFinalCount,
    metric_labels: {
      typea_enqueue_total: 'TypeA 入队总数',
      typea_ingesting_stale_total: 'TypeA ingesting 超时回收总数',
      typea_failed_final_total: 'TypeA 失败终态总数',
      typea_group_queue_jobs_total: 'TypeA 组级任务入队数',
      typea_single_queue_jobs_total: 'TypeA 单条任务入队数',
    },
      mode: 'round_robin_by_channel',
    });
}

// 任务定义入口：先扫描入库，再触发到期上传调度。
export async function scheduleRelayForDefinition(taskDefinitionId: bigint) {
  try {
    const enqueueSummary = await enqueueRelayAssetsFromTaskDefinition(taskDefinitionId);

    logger.info('[typea_metrics] relay scan summary', {
      typea_scan_files_total: enqueueSummary.scannedFiles,
      typea_enqueue_total: enqueueSummary.enqueuedTasks,
      typea_rejected_too_large_total: enqueueSummary.rejectedTooLarge ?? 0,
      typea_group_discovered_total: enqueueSummary.groupedDiscovered ?? 0,
      typea_group_discovered_distinct_total: enqueueSummary.groupedDistinct ?? 0,
      metric_labels: {
        typea_scan_files_total: 'TypeA 扫描文件总数',
        typea_enqueue_total: 'TypeA 入队总数（扫描阶段）',
        typea_rejected_too_large_total: 'TypeA 超大小文件拒绝总数（扫描阶段）',
        typea_group_discovered_total: 'TypeA grouped/single 扫描命中文件数',
        typea_group_discovered_distinct_total: 'TypeA grouped/single 扫描命中组数',
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

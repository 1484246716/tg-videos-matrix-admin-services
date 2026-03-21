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
      updatedAt: true,
    },
  });

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
    const sourceMeta = (asset.sourceMeta ?? {}) as Record<string, unknown>;
    const relayChannelId = sourceMeta.relayChannelId;
    if (typeof relayChannelId !== 'string' || !relayChannelId.trim()) continue;

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
        failedFinalCount += 1;
        continue;
      }

      staleRecoveredCount += 1;
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
    mode: 'round_robin_by_channel',
  });
}

export async function scheduleRelayForDefinition(taskDefinitionId: bigint) {
  try {
    const enqueueSummary = await enqueueRelayAssetsFromTaskDefinition(taskDefinitionId);

    logger.info('[typea_metrics] relay scan summary', {
      typea_scan_files_total: enqueueSummary.scannedFiles,
      typea_enqueue_total: enqueueSummary.enqueuedTasks,
      metric_labels: {
        typea_scan_files_total: 'TypeA 扫描文件总数',
        typea_enqueue_total: 'TypeA 入队总数（扫描阶段）',
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
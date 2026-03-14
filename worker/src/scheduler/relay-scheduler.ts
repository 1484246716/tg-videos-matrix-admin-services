import { MediaStatus } from '@prisma/client';
import { MAX_SCHEDULE_BATCH } from '../config/env';
import { prisma } from '../infra/prisma';
import { relayUploadQueue } from '../infra/redis';
import { logger } from '../logger';
import { updateTaskDefinitionRunStatus } from '../services/task-definition.service';
import { enqueueRelayAssetsFromTaskDefinition } from '../services/relay.service';

export async function scheduleDueRelayUploadTasks() {
  const staleIngestingBefore = new Date(Date.now() - 5 * 60 * 1000);

  const dueAssets = await prisma.mediaAsset.findMany({
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
    orderBy: { createdAt: 'asc' },
    take: MAX_SCHEDULE_BATCH,
    select: {
      id: true,
      channelId: true,
      originalName: true,
      status: true,
      sourceMeta: true,
      updatedAt: true,
    },
  });

  let queuedCount = 0;

  for (const asset of dueAssets) {
    const sourceMeta = (asset.sourceMeta ?? {}) as Record<string, unknown>;
    const relayChannelId = sourceMeta.relayChannelId;
    if (typeof relayChannelId !== 'string' || !relayChannelId.trim()) continue;

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

  if (queuedCount > 0) {
    logger.info('[scheduler] 已入队中转上传任务', { count: queuedCount });
  }
}

export async function scheduleRelayForDefinition(taskDefinitionId: bigint) {
  try {
    const enqueueSummary = await enqueueRelayAssetsFromTaskDefinition(taskDefinitionId);
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
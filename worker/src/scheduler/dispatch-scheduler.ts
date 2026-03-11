import { MediaStatus, TaskStatus } from '@prisma/client';
import { MAX_SCHEDULE_BATCH } from '../config/env';
import { prisma, getTaskDefinitionModel } from '../infra/prisma';
import { dispatchQueue } from '../infra/redis';
import { logger } from '../logger';
import { updateTaskDefinitionRunStatus } from '../services/task-definition.service';

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
    },
  });

  for (const task of dueTasks) {
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
  }

  if (dueTasks.length > 0) {
    logger.info('[scheduler] queued dispatch tasks', { count: dueTasks.length });
  }
}

export async function scheduleDispatchForDefinition(taskDefinitionId: bigint) {
  try {
    const definition = await getTaskDefinitionModel().findUnique({
      where: { id: taskDefinitionId },
      select: { priority: true },
    });

    if (!definition) {
      throw new Error(`Task Definition ${taskDefinitionId} not found`);
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
        message: 'Auto-scanned and queued dispatch tasks',
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    // eslint-disable-next-line no-console
    console.error(`[scheduler] dispatch_send taskDef=${taskDefinitionId} failed:`, error);
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'failed',
      summary: { executor: 'dispatch_send', error: message },
    });
  }
}

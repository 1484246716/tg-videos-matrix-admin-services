import { MAX_SCHEDULE_BATCH } from '../config/env';
import { prisma } from '../infra/prisma';
import { catalogQueue } from '../infra/redis';
import { logger } from '../logger';
import { updateTaskDefinitionRunStatus } from '../services/task-definition.service';

export async function scheduleDueCatalogTasks() {
  const now = new Date();

  const channels = await prisma.channel.findMany({
    where: {
      status: 'active',
      navEnabled: true,
    },
    select: {
      id: true,
      name: true,
      lastNavUpdateAt: true,
      navIntervalSec: true,
      navTemplateText: true,
      defaultBotId: true,
    },
  });

  let queuedCount = 0;

  for (const channel of channels) {
    if (!channel.navTemplateText || !channel.defaultBotId) continue;

    if (channel.lastNavUpdateAt) {
      const dueTime = channel.lastNavUpdateAt.getTime() + channel.navIntervalSec * 1000;
      if (now.getTime() < dueTime) continue;
    }

    const jobId = `catalog-${channel.id.toString()}`;
    const existingJob = await catalogQueue.getJob(jobId);
    if (existingJob) {
      const state = await existingJob.getState();
      if (state === 'failed') {
        await existingJob.remove();
      } else {
        continue;
      }
    }

    await catalogQueue.add(
      'catalog-publish',
      {
        channelIdRaw: channel.id.toString(),
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
    logger.info('[scheduler] 已入队频道导航任务', { count: queuedCount });
  }
}

export async function scheduleCatalogForDefinition(taskDefinitionId: bigint) {
  try {
    await scheduleDueCatalogTasks();
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'success',
      summary: {
        executor: 'catalog_publish',
        message: '频道导航调度完成',
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : '未知错误';
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'failed',
      summary: {
        executor: 'catalog_publish',
        error: `频道导航调度失败: ${message}`,
      },
    });

    throw error;
  }
}

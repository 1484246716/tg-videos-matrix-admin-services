import { CATALOG_CHANNEL_INTERVAL_GUARD_ENABLED } from '../config/env';
import { prisma } from '../infra/prisma';
import { catalogQueue } from '../infra/redis';
import { logger } from '../logger';
import { releaseChannelLock, tryAcquireChannelLock } from '../shared/channel-lock';
import { updateTaskDefinitionRunStatus } from '../services/task-definition.service';

function computeCatalogNextAllowedAt(args: {
  lastNavUpdateAt: Date | null;
  navIntervalSec: number;
  now: Date;
}) {
  if (!args.lastNavUpdateAt) return args.now;
  return new Date(args.lastNavUpdateAt.getTime() + Math.max(0, args.navIntervalSec) * 1000);
}

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

    if (CATALOG_CHANNEL_INTERVAL_GUARD_ENABLED) {
      const nextAllowedAt = computeCatalogNextAllowedAt({
        lastNavUpdateAt: channel.lastNavUpdateAt,
        navIntervalSec: channel.navIntervalSec,
        now,
      });
      if (nextAllowedAt.getTime() > now.getTime()) {
        logger.info('[scheduler] 目录任务未到频道更新窗口，跳过', {
          channelId: channel.id.toString(),
          navIntervalSec: channel.navIntervalSec,
          lastNavUpdateAt: channel.lastNavUpdateAt?.toISOString() ?? null,
          nextAllowedAt: nextAllowedAt.toISOString(),
        });
        continue;
      }
    }

    const lock = await tryAcquireChannelLock({ scope: 'catalog', channelId: channel.id });
    if (!lock.acquired) {
      logger.info('[scheduler] 目录任务跳过（频道锁未获取）', {
        channelId: channel.id.toString(),
        lockKey: lock.lockKey,
      });
      continue;
    }

    try {
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
    } finally {
      await releaseChannelLock({ lockKey: lock.lockKey, lockToken: lock.lockToken });
    }
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

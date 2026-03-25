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

function computeDispatchNextAllowedAt(args: {
  lastPostAt: Date | null;
  postIntervalSec: number;
  now: Date;
}) {
  if (!args.lastPostAt) return args.now;
  return new Date(args.lastPostAt.getTime() + Math.max(0, args.postIntervalSec) * 1000);
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
      channel: {
        select: {
          postIntervalSec: true,
          lastPostAt: true,
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

    const shouldBypassHead =
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

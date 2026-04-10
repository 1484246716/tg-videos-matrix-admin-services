import { prisma } from '../../infra/prisma';
import { cloneChannelIndexQueue } from '../../infra/redis';
import { logger } from '../../logger';
import { CloneChannelIndexJob } from '../types/clone-queue.types';

function normalizeChannelUsername(raw: string) {
  return raw.trim().replace(/^@+/, '').toLowerCase();
}

function buildCloneChannelIndexJobInput(params: {
  task: {
    id: bigint;
    recentLimit: number;
    contentTypes: string[];
  };
  runId: bigint;
  channel: {
    id: bigint;
    channelUsername: string;
    lastFetchedMessageId: bigint | null;
  };
}): CloneChannelIndexJob {
  return {
    taskId: params.task.id.toString(),
    runId: params.runId.toString(),
    channelUsername: normalizeChannelUsername(params.channel.channelUsername),
    channelId: params.channel.id.toString(),
    lastFetchedMessageId: params.channel.lastFetchedMessageId
      ? params.channel.lastFetchedMessageId.toString()
      : undefined,
    recentLimit: params.task.recentLimit,
    contentTypes: (params.task.contentTypes ?? []) as any,
    enqueuedAt: new Date().toISOString(),
    retryCount: 0,
  };
}

export async function scheduleCloneTasks() {
  const tasks = await prisma.cloneCrawlTask.findMany({
    where: { status: { in: ['running'] } },
    include: { channels: true },
    take: 20,
  });

  logger.info('[clone][任务调度/Task Scheduler] 扫描运行中任务 / scanning running tasks', {
    totalTasks: tasks.length,
  });

  for (const task of tasks) {
    const activeRun = await prisma.cloneCrawlRun.findFirst({
      where: {
        taskId: task.id,
        status: { in: ['pending', 'running'] },
      },
      select: { id: true, status: true, createdAt: true },
      orderBy: { createdAt: 'desc' },
    });

    if (activeRun) {
      const runAgeMs = Date.now() - new Date(activeRun.createdAt).getTime();

      if (activeRun.status === 'pending' && runAgeMs > 2 * 60_000) {
        await prisma.cloneCrawlRun.update({
          where: { id: activeRun.id },
          data: {
            status: 'failed',
            finishedAt: new Date(),
          },
        });

        logger.warn('[clone][任务调度/Task Scheduler] 检测到卡住的 pending run，已标记 failed / stale pending run marked failed', {
          taskId: task.id.toString(),
          taskName: task.name,
          staleRunId: activeRun.id.toString(),
          staleRunAgeMs: runAgeMs,
        });
      } else {
        logger.info('[clone][任务调度/Task Scheduler] 跳过创建 run（已有活动 run）/ skip run creation (active run exists)', {
          taskId: task.id.toString(),
          taskName: task.name,
          activeRunId: activeRun.id.toString(),
          activeRunStatus: activeRun.status,
          activeRunAgeMs: runAgeMs,
        });
        continue;
      }
    }

    if (task.scheduleType === 'once') {
      const hasFinishedRun = await prisma.cloneCrawlRun.findFirst({
        where: {
          taskId: task.id,
          status: { in: ['success', 'failed'] },
        },
        select: { id: true, status: true },
        orderBy: { createdAt: 'desc' },
      });

      if (hasFinishedRun) {
        logger.info('[clone][任务调度/Task Scheduler] 跳过创建 run（once 任务已有完成态）/ skip run creation (once task already finished)', {
          taskId: task.id.toString(),
          taskName: task.name,
          scheduleType: task.scheduleType,
          lastFinishedRunId: hasFinishedRun.id.toString(),
          lastFinishedRunStatus: hasFinishedRun.status,
        });
        continue;
      }
    }

    logger.info('[clone][任务调度/Task Scheduler] 准备创建 run / preparing run', {
      taskId: task.id.toString(),
      taskName: task.name,
      channelCount: task.channels.length,
      scheduleType: task.scheduleType,
      crawlMode: task.crawlMode,
    });

    const run = await prisma.cloneCrawlRun.create({
      data: {
        taskId: task.id,
        status: 'pending',
        startedAt: new Date(),
        channelTotal: task.channels.length,
      },
    });

    logger.info('[clone][任务调度/Task Scheduler] run 创建成功 / run created', {
      taskId: task.id.toString(),
      runId: run.id.toString(),
      channelTotal: task.channels.length,
    });

    for (const channel of task.channels) {
      const payload = buildCloneChannelIndexJobInput({
        task: {
          id: task.id,
          recentLimit: task.recentLimit,
          contentTypes: task.contentTypes,
        },
        runId: run.id,
        channel: {
          id: channel.id,
          channelUsername: channel.channelUsername,
          lastFetchedMessageId: channel.lastFetchedMessageId,
        },
      });

      await cloneChannelIndexQueue.add(
        'clone-channel-index',
        payload,
        { removeOnComplete: true, removeOnFail: 100 },
      );

      logger.info('[clone][任务调度/Task Scheduler] 频道索引已入队 / channel index enqueued', {
        taskId: task.id.toString(),
        runId: run.id.toString(),
        channelUsername: payload.channelUsername,
        channelId: payload.channelId,
        queue: cloneChannelIndexQueue.name,
      });
    }
  }

  logger.info('[clone][任务调度/Task Scheduler] 本轮调度完成 / scheduling tick finished', {
    totalTasks: tasks.length,
  });
}

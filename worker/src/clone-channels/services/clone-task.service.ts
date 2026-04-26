/**
 * Clone Channels 任务调度服务：负责生成 run 并派发频道索引任务。
 * 用于在 clone scheduler/worker 链路中推进 nextRunAt 与任务生命周期。
 */

import { Prisma } from '@prisma/client';
import { prisma } from '../../infra/prisma';
import { cloneChannelIndexQueue } from '../../infra/redis';
import { logger } from '../../logger';
import {
  CLONE_DAILY_DEFAULT_TIME,
  CLONE_DAILY_JITTER_SEC,
  CLONE_HOURLY_JITTER_SEC,
  CLONE_SCHEDULE_USE_NEXT_RUN_AT,
  CLONE_SCHEDULER_DUE_BATCH_SIZE,
} from '../../config/env';
import { CloneChannelIndexJob, CloneContentType } from '../types/clone-queue.types';

type CloneScheduleType = 'once' | 'interval' | 'hourly' | 'daily';

// 规范化频道用户名：去除 @ 前缀并统一小写。
function normalizeChannelUsername(raw: string) {
  return raw.trim().replace(/^@+/, '').toLowerCase();
}

// 构建频道索引任务入队 payload。
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
    contentTypes: (params.task.contentTypes ?? []) as CloneContentType[],
    enqueuedAt: new Date().toISOString(),
    retryCount: 0,
  };
}

// 解析每日运行时间（HH:mm）。
function parseDailyRunTime(raw?: string | null) {
  const fallback = CLONE_DAILY_DEFAULT_TIME;
  const source = (raw ?? fallback).trim();
  const m = source.match(/^(\d{2}):(\d{2})$/);
  if (!m) return { hour: 0, minute: 0 };
  const hour = Number(m[1]);
  const minute = Number(m[2]);
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return { hour: 0, minute: 0 };
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return { hour: 0, minute: 0 };
  return { hour, minute };
}

// 为计划时间添加随机抖动，避免任务同秒集中触发。
function applyJitter(date: Date, maxJitterSec: number) {
  if (!Number.isFinite(maxJitterSec) || maxJitterSec <= 0) return date;
  const jitterMs = Math.floor(Math.random() * (Math.floor(maxJitterSec) + 1)) * 1000;
  return new Date(date.getTime() + jitterMs);
}

// 获取指定时区下的年月日时分秒。
function zonedDateParts(date: Date, timeZone: string) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).formatToParts(date);

  // 读取指定类型的日期片段数值。
  const get = (type: string) => Number(parts.find((p) => p.type === type)?.value || '0');

  return {
    year: get('year'),
    month: get('month'),
    day: get('day'),
    hour: get('hour'),
    minute: get('minute'),
    second: get('second'),
  };
}

// 将时区本地时间换算为 UTC 时间点。
function zonedLocalToUtc(datePart: { year: number; month: number; day: number }, hour: number, minute: number, timeZone: string) {
  const baseUtc = Date.UTC(datePart.year, datePart.month - 1, datePart.day, hour, minute, 0, 0);
  const probe = new Date(baseUtc);
  const probeParts = zonedDateParts(probe, timeZone);
  const probeAsUtc = Date.UTC(
    probeParts.year,
    probeParts.month - 1,
    probeParts.day,
    probeParts.hour,
    probeParts.minute,
    probeParts.second,
    0,
  );
  const targetAsUtc = Date.UTC(datePart.year, datePart.month - 1, datePart.day, hour, minute, 0, 0);
  const offsetMs = probeAsUtc - baseUtc;
  return new Date(targetAsUtc - offsetMs);
}

// 计算任务下一次运行时间（once/hourly/daily）。
export function computeCloneTaskNextRunAt(params: {
  scheduleType: CloneScheduleType;
  timezone: string;
  dailyRunTime?: string | null;
  intervalSeconds?: number | null;
  from?: Date;
}): Date | null {
  const now = params.from ?? new Date();

  if (params.scheduleType === 'once') {
    return null;
  }

  if (params.scheduleType === 'interval') {
    const rawInterval = Number(params.intervalSeconds ?? 60);
    const intervalSeconds = Number.isFinite(rawInterval) ? Math.min(86400, Math.max(60, Math.floor(rawInterval))) : 60;
    return new Date(now.getTime() + intervalSeconds * 1000);
  }

  if (params.scheduleType === 'hourly') {
    return applyJitter(new Date(now.getTime() + 60 * 60 * 1000), CLONE_HOURLY_JITTER_SEC);
  }

  const timeZone = params.timezone || 'Asia/Shanghai';
  const { hour, minute } = parseDailyRunTime(params.dailyRunTime);
  const nowZoned = zonedDateParts(now, timeZone);

  const isAfterTodaySlot =
    nowZoned.hour > hour ||
    (nowZoned.hour === hour && nowZoned.minute > minute) ||
    (nowZoned.hour === hour && nowZoned.minute === minute && nowZoned.second > 0);

  const localDate = new Date(Date.UTC(nowZoned.year, nowZoned.month - 1, nowZoned.day));
  if (isAfterTodaySlot) {
    localDate.setUTCDate(localDate.getUTCDate() + 1);
  }

  const target = zonedLocalToUtc(
    {
      year: localDate.getUTCFullYear(),
      month: localDate.getUTCMonth() + 1,
      day: localDate.getUTCDate(),
    },
    hour,
    minute,
    timeZone,
  );

  return applyJitter(target, CLONE_DAILY_JITTER_SEC);
}

// 标记 run 完成并推进任务 nextRunAt。
export async function markCloneTaskRunFinished(params: {
  taskId: bigint;
  scheduleType: CloneScheduleType;
  timezone: string;
  dailyRunTime?: string | null;
  intervalSeconds?: number | null;
  tx?: Prisma.TransactionClient;
}) {
  const db = params.tx ?? prisma;
  const nextRunAt = computeCloneTaskNextRunAt({
    scheduleType: params.scheduleType,
    timezone: params.timezone,
    dailyRunTime: params.dailyRunTime,
    intervalSeconds: params.intervalSeconds,
  });

  await (db as any).cloneCrawlTask.update({
    where: { id: params.taskId },
    data: {
      lastRunAt: new Date(),
      nextRunAt,
      status: params.scheduleType === 'once' ? 'completed' : undefined,
    },
  });

  logger.info('[clone][任务调度/Task Scheduler] 任务 nextRunAt 已推进 / task nextRunAt advanced', {
    taskId: params.taskId.toString(),
    scheduleType: params.scheduleType,
    nextRunAt: nextRunAt?.toISOString() ?? null,
  });
}

// clone 调度主流程：扫描到点任务、创建 run 并派发频道索引。
export async function scheduleCloneTasks() {
  const now = new Date();

  const dueWhere = CLONE_SCHEDULE_USE_NEXT_RUN_AT
    ? {
        status: { in: ['running'] as const },
        nextRunAt: { lte: now },
      }
    : {
        status: { in: ['running'] as const },
      };

  const tasks = await (prisma as any).cloneCrawlTask.findMany({
    where: dueWhere,
    include: { channels: true },
    orderBy: CLONE_SCHEDULE_USE_NEXT_RUN_AT ? { nextRunAt: 'asc' } : { updatedAt: 'desc' },
    take: CLONE_SCHEDULER_DUE_BATCH_SIZE,
  });

  logger.info('[clone][任务调度/Task Scheduler] 扫描到点任务 / scanning due tasks', {
    dueCount: tasks.length,
    batchSize: CLONE_SCHEDULER_DUE_BATCH_SIZE,
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

    const acquireWhere = CLONE_SCHEDULE_USE_NEXT_RUN_AT
      ? {
          id: task.id,
          status: 'running',
          nextRunAt: { lte: now },
        }
      : {
          id: task.id,
          status: 'running',
        };

    const acquired = await (prisma as any).cloneCrawlTask.updateMany({
      where: acquireWhere,
      data: {
        lastRunAt: now,
      },
    });

    if (acquired.count < 1) {
      logger.info('[clone][任务调度/Task Scheduler] 跳过创建 run（未抢占到执行权）/ skip run creation (not acquired)', {
        taskId: task.id.toString(),
      });
      continue;
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
    dueCount: tasks.length,
  });
}

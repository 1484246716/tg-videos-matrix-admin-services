/**
 * Clone Channels 资源守卫服务：在下载前做磁盘与并发准入判断。
 * 用于在 clone 调度/执行链路中提前拦截高风险任务并写入暂停状态。
 */

import { statfs } from 'node:fs/promises';
import path from 'node:path';
import { prisma } from '../../infra/prisma';
import {
  CLONE_DISK_FUSE_THRESHOLD,
  CLONE_DOWNLOAD_CHANNEL_CONCURRENCY,
  CLONE_DOWNLOAD_GLOBAL_CONCURRENCY,
} from '../constants/clone-queue.constants';
import { logger } from '../../logger';
import { GuardDecision, GuardReason, ResourceSnapshot } from '../types/clone-queue.types';

// 获取目标路径的磁盘使用率百分比。
async function getDiskUsagePercent(targetPath?: string | null) {
  try {
    const safePath = targetPath && targetPath.trim() ? targetPath : process.cwd();
    const normalized = path.resolve(safePath);
    const fsStat = await statfs(normalized);

    const total = Number(fsStat.blocks) * Number(fsStat.bsize);
    const free = Number(fsStat.bavail) * Number(fsStat.bsize);
    const used = total - free;

    if (!Number.isFinite(total) || total <= 0) return 0;
    const percent = (used / total) * 100;
    return Math.max(0, Math.min(100, Number(percent.toFixed(2))));
  } catch {
    return 0;
  }
}

// 采集运行时资源快照（磁盘占用、全局与频道下载并发）。
export async function collectRuntimeResourceSnapshot(params: {
  targetPath: string;
  channelUsername?: string;
  excludeItemId?: bigint;
}): Promise<ResourceSnapshot> {
  const diskUsagePercent = await getDiskUsagePercent(params.targetPath);

  const baseDownloadingWhere = {
    downloadStatus: 'downloading' as const,
    ...(params.excludeItemId ? { id: { not: params.excludeItemId } } : {}),
  };

  const [globalDownloadingCount, channelDownloadingCount] = await Promise.all([
    prisma.cloneCrawlItem.count({ where: baseDownloadingWhere }),
    params.channelUsername
      ? prisma.cloneCrawlItem.count({
          where: {
            ...baseDownloadingWhere,
            channelUsername: params.channelUsername,
          },
        })
      : Promise.resolve(0),
  ]);

  return {
    diskUsagePercent,
    inflightBytes: BigInt(0),
    globalDownloadingCount,
    channelDownloadingCount,
  };
}

// 执行下载守卫检查：磁盘阈值、全局并发、频道并发。
export async function checkDownloadGuards(params: {
  taskId: bigint;
  runId: bigint;
  itemId?: bigint;
  channelUsername: string;
  targetPath: string;
  expectedFileSize?: bigint;
}): Promise<GuardDecision> {
  const snapshot = await collectRuntimeResourceSnapshot({
    targetPath: params.targetPath,
    channelUsername: params.channelUsername,
    excludeItemId: params.itemId,
  });

  if (snapshot.diskUsagePercent >= CLONE_DISK_FUSE_THRESHOLD) {
    return {
      pass: false,
      reason: 'disk_guard_triggered',
      retryDelayMs: 30_000,
    };
  }

  const taskConfig = await prisma.cloneCrawlTask.findUnique({
    where: { id: params.taskId },
    select: { globalDownloadConcurrency: true },
  });

  const globalLimit =
    taskConfig?.globalDownloadConcurrency && taskConfig.globalDownloadConcurrency > 0
      ? taskConfig.globalDownloadConcurrency
      : CLONE_DOWNLOAD_GLOBAL_CONCURRENCY;

  if (snapshot.globalDownloadingCount >= globalLimit) {
    return {
      pass: false,
      reason: 'global_concurrency_exceeded',
      retryDelayMs: 30_000,
    };
  }

  if (snapshot.channelDownloadingCount >= CLONE_DOWNLOAD_CHANNEL_CONCURRENCY) {
    return {
      pass: false,
      reason: 'per_channel_concurrency_exceeded',
      retryDelayMs: 30_000,
    };
  }

  return {
    pass: true,
    diskUsagePercent: snapshot.diskUsagePercent,
  };
}

// 记录守卫触发结果：将条目标记为 paused_by_guard 并写告警日志。
export async function recordGuardTriggered(params: {
  itemId: bigint;
  reason: GuardReason;
  detail: string;
}): Promise<void> {
  await prisma.cloneCrawlItem.updateMany({
    where: { id: params.itemId },
    data: {
      downloadStatus: 'paused_by_guard',
      downloadErrorCode: params.reason,
      downloadError: params.detail,
    },
  });

  logger.warn('[clone][guard] triggered', {
    itemId: params.itemId.toString(),
    reason: params.reason,
    detail: params.detail,
  });
}

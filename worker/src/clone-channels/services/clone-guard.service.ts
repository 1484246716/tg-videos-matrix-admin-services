import { statfs } from 'node:fs/promises';
import path from 'node:path';
import { prisma } from '../../infra/prisma';
import {
  CLONE_DISK_FUSE_THRESHOLD,
  CLONE_DOWNLOAD_GLOBAL_CONCURRENCY,
} from '../constants/clone-queue.constants';
import { logger } from '../../logger';
import { GuardDecision, GuardReason, ResourceSnapshot } from '../types/clone-queue.types';

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

export async function collectRuntimeResourceSnapshot(params: {
  targetPath: string;
  channelUsername?: string;
}): Promise<ResourceSnapshot> {
  const diskUsagePercent = await getDiskUsagePercent(params.targetPath);

  const [globalDownloadingCount, channelDownloadingCount] = await Promise.all([
    prisma.cloneCrawlItem.count({ where: { downloadStatus: 'downloading' } }),
    params.channelUsername
      ? prisma.cloneCrawlItem.count({
          where: {
            downloadStatus: 'downloading',
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
  });

  if (snapshot.diskUsagePercent >= CLONE_DISK_FUSE_THRESHOLD) {
    return {
      pass: false,
      reason: 'disk_guard_triggered',
      retryDelayMs: 30_000,
    };
  }

  if (snapshot.globalDownloadingCount >= CLONE_DOWNLOAD_GLOBAL_CONCURRENCY) {
    return {
      pass: false,
      reason: 'global_concurrency_exceeded',
      retryDelayMs: 10_000,
    };
  }

  return {
    pass: true,
    diskUsagePercent: snapshot.diskUsagePercent,
  };
}

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

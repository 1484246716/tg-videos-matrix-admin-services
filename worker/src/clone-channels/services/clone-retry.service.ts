import { cloneChannelIndexQueue, cloneVideoDownloadQueue } from '../../infra/redis';
import { prisma } from '../../infra/prisma';
import { logger } from '../../logger';
import {
  CLONE_RETRY_BASE_DELAY_MS,
  CLONE_RETRY_MAX,
  CLONE_RETRY_MAX_DELAY_MS,
} from '../constants/clone-queue.constants';
import { CloneRetryJob, CloneRetryReason } from '../types/clone-queue.types';

function isRetryable(reason: CloneRetryReason | string) {
  const nonRetryableReasons = new Set<string>([
    'auth_invalid',
    'channel_unreachable',
    'file_too_large',
  ]);
  return !nonRetryableReasons.has(reason);
}

function computeRetryDelayMs(params: {
  retryCount: number;
  retryAfterSec?: number;
  maxDelayMs?: number;
}) {
  const maxDelayMs = params.maxDelayMs ?? CLONE_RETRY_MAX_DELAY_MS;

  if (Number.isFinite(params.retryAfterSec) && (params.retryAfterSec as number) > 0) {
    return Math.min(maxDelayMs, Math.floor((params.retryAfterSec as number) * 1000));
  }

  const base = CLONE_RETRY_BASE_DELAY_MS;
  const expDelay = 2 ** Math.max(1, params.retryCount) * base;
  return Math.min(maxDelayMs, expDelay);
}

function getPayloadTaskId(job: CloneRetryJob): bigint | null {
  const raw = (job.payload as { taskId?: unknown })?.taskId;
  if (typeof raw !== 'string' && typeof raw !== 'number' && typeof raw !== 'bigint') return null;
  try {
    return BigInt(raw);
  } catch {
    return null;
  }
}

async function resolveRetryMax(job: CloneRetryJob) {
  const taskId = getPayloadTaskId(job);
  if (!taskId) return CLONE_RETRY_MAX;

  const task = await prisma.cloneCrawlTask.findUnique({
    where: { id: taskId },
    select: { retryMax: true },
  });

  if (!task?.retryMax || task.retryMax < 0) return CLONE_RETRY_MAX;
  return task.retryMax;
}

async function markDownloadExhaustedIfPossible(job: CloneRetryJob, reason: string, retryCount: number) {
  if (job.queue !== 'download') return;

  const rawItemId = (job.payload as { itemId?: unknown })?.itemId;
  if (typeof rawItemId !== 'string' && typeof rawItemId !== 'number' && typeof rawItemId !== 'bigint') return;

  let itemId: bigint;
  try {
    itemId = BigInt(rawItemId);
  } catch {
    return;
  }

  await prisma.cloneCrawlItem.updateMany({
    where: { id: itemId },
    data: {
      downloadStatus: 'failed_final',
      downloadErrorCode: 'retry_exhausted',
      downloadError: `retry exhausted after ${retryCount} attempts, last_reason=${reason}`,
    },
  });
}

export async function processCloneRetry(job: CloneRetryJob) {
  const retryCount = Math.max(0, job.retryCount ?? 0);

  if (job.nonRetryable || !isRetryable(job.reason)) {
    logger.warn('[clone] non-retryable error skipped', {
      queue: job.queue,
      reason: job.reason,
      retryCount,
      nonRetryable: job.nonRetryable ?? false,
    });
    return;
  }

  const retryMax = await resolveRetryMax(job);

  if (retryCount >= retryMax) {
    await markDownloadExhaustedIfPossible(job, String(job.reason), retryCount);
    logger.warn('[clone] retry exhausted', {
      queue: job.queue,
      reason: job.reason,
      retryCount,
      retryMax,
    });
    return;
  }

  const nextRetryCount = retryCount + 1;
  const delayMs = computeRetryDelayMs({
    retryCount: nextRetryCount,
    retryAfterSec: job.retryAfterSec,
  });

  if (job.queue === 'index') {
    await cloneChannelIndexQueue.add('clone-channel-index-retry', job.payload, {
      delay: delayMs,
      removeOnComplete: true,
      removeOnFail: 100,
    });
  }

  if (job.queue === 'download') {
    await cloneVideoDownloadQueue.add('clone-video-download-retry', job.payload, {
      delay: delayMs,
      removeOnComplete: true,
      removeOnFail: 100,
    });
  }

  logger.info('[clone] retry requeued', {
    queue: job.queue,
    reason: job.reason,
    retryCount: nextRetryCount,
    retryMax,
    delayMs,
  });
}

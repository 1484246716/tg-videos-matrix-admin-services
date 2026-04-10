import { cloneChannelIndexQueue, cloneVideoDownloadQueue } from '../../infra/redis';
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

  if (retryCount >= CLONE_RETRY_MAX) {
    logger.warn('[clone] retry exhausted', {
      queue: job.queue,
      reason: job.reason,
      retryCount,
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
    delayMs,
  });
}

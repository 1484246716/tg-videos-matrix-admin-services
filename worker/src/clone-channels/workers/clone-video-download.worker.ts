/**
 * Clone Channels 下载 Worker：消费下载队列并执行媒体下载。
 * 负责 payload 校验、调用下载服务以及失败日志上报。
 */

import { Worker } from 'bullmq';
import { cloneMediaDownloadQueue, connection } from '../../infra/redis';
import { logger, logError } from '../../logger';
import { CLONE_DOWNLOAD_GLOBAL_CONCURRENCY } from '../constants/clone-queue.constants';
import { processCloneMediaDownload } from '../services/clone-download.service';

// 校验下载任务是否具备必需字段。
function hasRequiredDownloadPayload(data: any) {
  const hasTaskId = typeof data?.taskId === 'string' || typeof data?.taskId === 'number' || typeof data?.taskId === 'bigint';
  const hasRunId = typeof data?.runId === 'string' || typeof data?.runId === 'number' || typeof data?.runId === 'bigint';
  const hasItemId = typeof data?.itemId === 'string' || typeof data?.itemId === 'number' || typeof data?.itemId === 'bigint';
  return hasTaskId && hasRunId && hasItemId;
}

export const cloneMediaDownloadWorker = new Worker(
  cloneMediaDownloadQueue.name,
  async (job) => {
    if (!hasRequiredDownloadPayload(job.data)) {
      logger.warn('[clone-media-download.worker] invalid payload skipped', {
        jobId: job.id,
        data: job.data,
      });
      return;
    }

    const startedAt = Date.now();
    await processCloneMediaDownload(job.data, String(job.id ?? ''));
    logger.info('[clone-media-download.worker] job completed', {
      jobId: job.id,
      taskId: job.data?.taskId,
      runId: job.data?.runId,
      itemId: job.data?.itemId,
      channelUsername: job.data?.channelUsername,
      attempt: job.attemptsStarted,
      elapsedMs: Date.now() - startedAt,
    });
  },
  { connection: connection as any, concurrency: CLONE_DOWNLOAD_GLOBAL_CONCURRENCY },
);

cloneMediaDownloadWorker.on('failed', (job, err) => {
  logError('[clone-media-download.worker] failed', err);
  logger.warn('[clone-media-download.worker] job failed', {
    jobId: job?.id,
    taskId: job?.data?.taskId,
    runId: job?.data?.runId,
    itemId: job?.data?.itemId,
    channelUsername: job?.data?.channelUsername,
    attempt: job?.attemptsStarted,
    error: err instanceof Error ? err.message : String(err),
  });
});

cloneMediaDownloadWorker.on('ready', () => {
  logger.info('[clone-media-download.worker] ready');
});

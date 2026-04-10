import { Worker } from 'bullmq';
import { cloneVideoDownloadQueue, connection } from '../../infra/redis';
import { logger, logError } from '../../logger';
import { CLONE_DOWNLOAD_GLOBAL_CONCURRENCY } from '../constants/clone-queue.constants';
import { processCloneVideoDownload } from '../services/clone-download.service';

export const cloneVideoDownloadWorker = new Worker(
  cloneVideoDownloadQueue.name,
  async (job) => {
    const startedAt = Date.now();
    await processCloneVideoDownload(job.data);
    logger.info('[clone-video-download.worker] job completed', {
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

cloneVideoDownloadWorker.on('failed', (job, err) => {
  logError('[clone-video-download.worker] failed', err);
  logger.warn('[clone-video-download.worker] job failed', {
    jobId: job?.id,
    taskId: job?.data?.taskId,
    runId: job?.data?.runId,
    itemId: job?.data?.itemId,
    channelUsername: job?.data?.channelUsername,
    attempt: job?.attemptsStarted,
    error: err instanceof Error ? err.message : String(err),
  });
});

cloneVideoDownloadWorker.on('ready', () => {
  logger.info('[clone-video-download.worker] ready');
});

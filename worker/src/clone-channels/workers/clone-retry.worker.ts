import { Worker } from 'bullmq';
import { cloneRetryQueue, connection } from '../../infra/redis';
import { logger, logError } from '../../logger';
import { processCloneRetry } from '../services/clone-retry.service';

export const cloneRetryWorker = new Worker(
  cloneRetryQueue.name,
  async (job) => {
    await processCloneRetry(job.data);
  },
  { connection: connection as any, concurrency: 1 },
);

cloneRetryWorker.on('failed', (_job, err) => {
  logError('[clone-retry.worker] failed', err);
});

cloneRetryWorker.on('ready', () => {
  logger.info('[clone-retry.worker] ready');
});

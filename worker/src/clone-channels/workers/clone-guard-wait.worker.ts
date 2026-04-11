import { Worker } from 'bullmq';
import { cloneGuardWaitQueue, connection } from '../../infra/redis';
import { logger, logError } from '../../logger';
import { processCloneGuardWait } from '../services/clone-guard-wait.service';

export const cloneGuardWaitWorker = new Worker(
  cloneGuardWaitQueue.name,
  async (job) => {
    await processCloneGuardWait(job.data);
  },
  { connection: connection as any, concurrency: 4 },
);

cloneGuardWaitWorker.on('failed', (_job, err) => {
  logError('[clone-guard-wait.worker] failed', err);
});

cloneGuardWaitWorker.on('ready', () => {
  logger.info('[clone-guard-wait.worker] ready');
});

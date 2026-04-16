import { Worker } from 'bullmq';
import { cloneGroupL1DispatchQueue, connection } from '../../infra/redis';
import { logger, logError } from '../../logger';
import { dispatchCloneGroupOneRound } from '../services/clone-group-scheduler.service';

export const cloneGroupL1DispatchWorker = new Worker(
  cloneGroupL1DispatchQueue.name,
  async () => {
    await dispatchCloneGroupOneRound();
  },
  { connection: connection as any, concurrency: 1 },
);

cloneGroupL1DispatchWorker.on('failed', (_job, err) => {
  logError('[clone-group-l1-dispatch.worker] failed', err);
});

cloneGroupL1DispatchWorker.on('ready', () => {
  logger.info('[clone-group-l1-dispatch.worker] ready');
});

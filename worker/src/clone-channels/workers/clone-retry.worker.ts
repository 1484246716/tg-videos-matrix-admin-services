/**
 * Clone Channels 重试 Worker：消费重试队列并回推分组分发。
 * 负责执行 retry 逻辑，并在下载重试后触发下一轮 L1 调度。
 */

import { Worker } from 'bullmq';
import { cloneGroupL1DispatchQueue, cloneRetryQueue, connection } from '../../infra/redis';
import { logger, logError } from '../../logger';
import { processCloneRetry } from '../services/clone-retry.service';

export const cloneRetryWorker = new Worker(
  cloneRetryQueue.name,
  async (job) => {
    await processCloneRetry(job.data);

    if (job.data?.queue === 'download') {
      await cloneGroupL1DispatchQueue.add(
        'clone-group-l1-dispatch-tick',
        { source: 'clone-retry', at: new Date().toISOString() },
        { removeOnComplete: true, removeOnFail: 100 },
      );
    }
  },
  { connection: connection as any, concurrency: 1 },
);

cloneRetryWorker.on('failed', (_job, err) => {
  logError('[clone-retry.worker] failed', err);
});

cloneRetryWorker.on('ready', () => {
  logger.info('[clone-retry.worker] ready');
});

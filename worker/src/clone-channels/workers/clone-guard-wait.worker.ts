/**
 * Clone Channels guard-wait Worker：处理被守卫暂停后的恢复任务。
 * 负责消费 guard-wait 队列并调用公平恢复服务。
 */

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

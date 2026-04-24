/**
 * Clone Channels 调度 Worker：触发定时 crawl 调度入口。
 * 负责在 bootstrap 后消费调度队列并调用 clone task 调度服务。
 */

import { Worker } from 'bullmq';
import { connection, cloneCrawlScheduleQueue } from '../../infra/redis';
import { logger, logError } from '../../logger';
import { scheduleCloneTasks } from '../services/clone-task.service';

export const cloneCrawlScheduleWorker = new Worker(
  cloneCrawlScheduleQueue.name,
  async (job) => {
    logger.info('[clone][调度/Scheduler] 开始执行 crawl 调度 / start crawl scheduling', {
      jobId: job.id,
      queue: cloneCrawlScheduleQueue.name,
      data: job.data,
    });
    await scheduleCloneTasks();
    logger.info('[clone][调度/Scheduler] crawl 调度执行完成 / crawl scheduling finished', {
      jobId: job.id,
    });
  },
  { connection: connection as any, concurrency: 1 },
);

cloneCrawlScheduleWorker.on('failed', (_job, err) => {
  logError('[clone-crawl-schedule.worker] failed', err);
});

cloneCrawlScheduleWorker.on('ready', () => {
  logger.info('[clone-crawl-schedule.worker] ready');
});

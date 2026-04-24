/**
 * Clone Channels L2 下载 Worker：执行分组媒体下载任务。
 * 负责消费 L2 队列、调用下载服务，并在完成/失败后驱动下一轮 L1 分发。
 */

import { Worker } from 'bullmq';
import { cloneGroupL1DispatchQueue, cloneGroupL2DownloadQueue, connection } from '../../infra/redis';
import { logger, logError } from '../../logger';
import { processCloneMediaDownload } from '../services/clone-download.service';
import {
  getCloneGroupL2Concurrency,
  onCloneGroupL2Done,
} from '../services/clone-group-scheduler.service';

// 校验下载任务是否具备必需字段。
function hasRequiredDownloadPayload(data: any) {
  const hasTaskId = typeof data?.taskId === 'string' || typeof data?.taskId === 'number' || typeof data?.taskId === 'bigint';
  const hasRunId = typeof data?.runId === 'string' || typeof data?.runId === 'number' || typeof data?.runId === 'bigint';
  const hasItemId = typeof data?.itemId === 'string' || typeof data?.itemId === 'number' || typeof data?.itemId === 'bigint';
  return hasTaskId && hasRunId && hasItemId;
}

export const cloneGroupL2DownloadWorker = new Worker(
  cloneGroupL2DownloadQueue.name,
  async (job) => {
    if (!hasRequiredDownloadPayload(job.data)) {
      logger.warn('[clone-group-l2-download.worker] invalid payload skipped', {
        jobId: job.id,
        data: job.data,
      });
      return;
    }

    logger.info('[clone][l1l2] l2 start download', {
      jobId: job.id,
      itemId: job.data?.itemId,
      groupKey: job.data?.groupKey,
      groupedId: job.data?.groupedId,
    });
    await processCloneMediaDownload(job.data, String(job.id ?? ''));
  },
  { connection: connection as any, concurrency: getCloneGroupL2Concurrency() },
);

cloneGroupL2DownloadWorker.on('completed', async (job) => {
  logger.info('[clone][l1l2] l2 completed', {
    jobId: job?.id,
    itemId: job?.data?.itemId,
    groupKey: job?.data?.groupKey,
  });
  await onCloneGroupL2Done();
  await cloneGroupL1DispatchQueue.add(
    'clone-group-l1-dispatch-tick',
    { source: 'clone-group-l2-completed', at: new Date().toISOString() },
    { removeOnComplete: true, removeOnFail: 100 },
  );
});

cloneGroupL2DownloadWorker.on('failed', async (job, err) => {
  logger.warn('[clone][l1l2] l2 failed', {
    jobId: job?.id,
    itemId: job?.data?.itemId,
    groupKey: job?.data?.groupKey,
    error: err instanceof Error ? err.message : String(err),
  });
  await onCloneGroupL2Done();
  await cloneGroupL1DispatchQueue.add(
    'clone-group-l1-dispatch-tick',
    { source: 'clone-group-l2-failed', at: new Date().toISOString() },
    { removeOnComplete: true, removeOnFail: 100 },
  );
  logError('[clone-group-l2-download.worker] failed', err);
});

cloneGroupL2DownloadWorker.on('ready', () => {
  logger.info('[clone-group-l2-download.worker] ready');
});

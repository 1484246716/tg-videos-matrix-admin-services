/**
 * Clone Channels 频道索引 Worker：消费索引队列并调用索引服务。
 * 负责在 bootstrap 后执行 clone channel index job 的校验、处理与失败上报。
 */

import { Worker } from 'bullmq';
import { cloneChannelIndexQueue, connection } from '../../infra/redis';
import { logger, logError } from '../../logger';
import { processCloneChannelIndex } from '../services/clone-index.service';

// 校验索引任务是否具备必需字段。
function hasRequiredIndexPayload(data: any) {
  const hasTaskId = typeof data?.taskId === 'string' || typeof data?.taskId === 'number' || typeof data?.taskId === 'bigint';
  const hasRunId = typeof data?.runId === 'string' || typeof data?.runId === 'number' || typeof data?.runId === 'bigint';
  const hasChannelUsername = typeof data?.channelUsername === 'string' && data.channelUsername.trim().length > 0;
  return hasTaskId && hasRunId && hasChannelUsername;
}

export const cloneChannelIndexWorker = new Worker(
  cloneChannelIndexQueue.name,
  async (job) => {
    if (!hasRequiredIndexPayload(job.data)) {
      logger.warn('[clone-channel-index.worker] invalid payload skipped', {
        jobId: job.id,
        data: job.data,
      });
      return;
    }

    const startedAt = Date.now();
    await processCloneChannelIndex(job.data);
    logger.info('[clone-channel-index.worker] job completed', {
      jobId: job.id,
      taskId: job.data?.taskId,
      runId: job.data?.runId,
      channelUsername: job.data?.channelUsername,
      attempt: job.attemptsStarted,
      durationMs: Date.now() - startedAt,
    });
  },
  { connection: connection as any, concurrency: 4 },
);

cloneChannelIndexWorker.on('failed', (job, err) => {
  const msg = err instanceof Error ? err.message : String(err);
  const lowered = msg.toLowerCase();
  const errorCode = lowered.includes('floodwait') || lowered.includes('flood_wait')
    ? 'flood_wait'
    : lowered.includes('timeout') || lowered.includes('network') || lowered.includes('socket')
      ? 'network_timeout'
      : lowered.includes('auth_invalid') || lowered.includes('auth')
        ? 'auth_invalid'
        : 'index_unknown_error';

  logError('[clone-channel-index.worker] failed', err);
  logger.warn('[clone-channel-index.worker] job failed', {
    jobId: job?.id,
    taskId: job?.data?.taskId,
    runId: job?.data?.runId,
    channelUsername: job?.data?.channelUsername,
    attempt: job?.attemptsStarted,
    errorCode,
    error: msg,
  });
});

cloneChannelIndexWorker.on('ready', () => {
  logger.info('[clone-channel-index.worker] ready');
});

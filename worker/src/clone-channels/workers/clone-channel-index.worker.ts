import { Worker } from 'bullmq';
import { cloneChannelIndexQueue, connection } from '../../infra/redis';
import { logger, logError } from '../../logger';
import { processCloneChannelIndex } from '../services/clone-index.service';

export const cloneChannelIndexWorker = new Worker(
  cloneChannelIndexQueue.name,
  async (job) => {
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

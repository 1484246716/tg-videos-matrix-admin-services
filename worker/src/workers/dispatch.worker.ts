import { Worker } from 'bullmq';
import { connection } from '../infra/redis';
import { logger, logError } from '../logger';
import { handleDispatchJob } from '../services/dispatch.service';

export const dispatchWorker = new Worker(
  'q_dispatch',
  async (job) => {
    if (job.name === 'bootstrap-check') {
      return { ok: true, skipped: true, reason: 'bootstrap-check' };
    }

    const dispatchTaskIdRaw = job.data.dispatchTaskId as string | undefined;
    if (!dispatchTaskIdRaw) {
      throw new Error('任务负载缺少 dispatchTaskId');
    }

    return handleDispatchJob(dispatchTaskIdRaw, String(job.id), job.attemptsMade);
  },
  { connection: connection as any, concurrency: 5 },
);

dispatchWorker.on('completed', (job) => {
  logger.info('[q_dispatch] 任务完成', { jobId: String(job.id) });
});

dispatchWorker.on('failed', (job, err) => {
  logError('[q_dispatch] 任务失败', {
    jobId: job?.id ? String(job.id) : null,
    error: err,
  });
});

/**
 * Dispatch Worker：消费分发队列并执行单条/分组分发任务。
 * 在 bootstrap 注册后由 BullMQ 驱动，负责接收 job 并调用 dispatch service。
 */

import { Worker } from 'bullmq';
import { connection } from '../infra/redis';
import { logger, logError, toReadableErrorSummary } from '../logger';
import { handleDispatchJob, handleDispatchGroupJob } from '../services/dispatch.service';

export const dispatchWorker = new Worker(
  'q_dispatch',
  async (job) => {
    if (job.name === 'bootstrap-check') {
      return { ok: true, skipped: true, reason: 'bootstrap-check' };
    }

    if (job.name === 'dispatch-send-group') {
      const dispatchTaskIdRaw = job.data.dispatchTaskId as string | undefined;
      if (!dispatchTaskIdRaw) {
        throw new Error('组任务负载缺少 dispatchTaskId');
      }
      return handleDispatchGroupJob(dispatchTaskIdRaw, String(job.id), job.attemptsMade);
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
  let errorJson: string | null = null;
  if (!(err instanceof Error)) {
    try {
      errorJson = JSON.stringify(err);
    } catch {
      errorJson = null;
    }
  }

  const errorInfo = err instanceof Error
    ? { name: err.name, message: err.message, stack: err.stack }
    : { raw: err, string: toReadableErrorSummary(err), json: errorJson };

  logError('[q_dispatch] 任务失败', {
    jobId: job?.id ? String(job.id) : null,
    jobName: job?.name ?? null,
    attemptsMade: job?.attemptsMade ?? null,
    dispatchTaskId: (job?.data?.dispatchTaskId as string | undefined) ?? null,
    groupKey: (job?.data?.groupKey as string | undefined) ?? null,
    errorSummary: toReadableErrorSummary(err),
    error: errorInfo,
  });
});

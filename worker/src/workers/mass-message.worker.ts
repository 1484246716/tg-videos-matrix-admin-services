/**
 * Mass Message Worker：消费群发队列并处理单条群发项。
 * 在 bootstrap 注册后由 BullMQ 驱动，负责接收 job 并调用 mass-message service。
 */

import { Worker } from 'bullmq';
import { connection } from '../infra/redis';
import { logger, logError } from '../logger';
import { handleMassMessageItem } from '../services/mass-message.service';

export const massMessageWorker = new Worker(
  'q_mass_message',
  async (job) => {
    if (job.name === 'bootstrap-check') {
      return { ok: true, skipped: true, reason: 'bootstrap-check' };
    }

    const itemId = job.data.itemId as string | undefined;
    if (!itemId) {
      throw new Error('任务负载缺少 itemId');
    }

    return handleMassMessageItem(itemId);
  },
  { connection: connection as any, concurrency: 3 },
);

massMessageWorker.on('completed', (job) => {
  logger.info('[q_mass_message] 任务完成', { jobId: String(job.id) });
});

massMessageWorker.on('failed', (job, err) => {
  logError('[q_mass_message] 任务失败', {
    jobId: job?.id ? String(job.id) : null,
    error: err,
  });
});


import { Worker } from 'bullmq';
import { connection } from '../infra/redis';
import { logger, logError } from '../logger';
import { handleCatalogJob } from '../services/catalog.service';

export const catalogWorker = new Worker(
  'q_catalog',
  async (job) => {
    if (job.name === 'bootstrap-check') {
      return { ok: true, skipped: true, reason: 'bootstrap-check' };
    }

    const channelIdRaw = job.data.channelIdRaw as string | undefined;
    if (!channelIdRaw) {
      throw new Error('任务负载缺少 channelIdRaw');
    }

    const selfHealOnly = Boolean((job.data as { selfHealOnly?: boolean } | undefined)?.selfHealOnly);

    return handleCatalogJob(channelIdRaw, { selfHealOnly });
  },
  { connection: connection as any, concurrency: 3 },
);

catalogWorker.on('completed', (job) => {
  logger.info('[q_catalog] 任务完成', { jobId: String(job.id) });
});

catalogWorker.on('failed', (job, err) => {
  logger.error('[q_catalog] 任务失败', {
    jobId: job?.id ? String(job.id) : null,
    jobName: job?.name ?? null,
    channelIdRaw: (job?.data as any)?.channelIdRaw ?? null,
    errName: err?.name ?? null,
    errMessage: err?.message ?? null,
    errStack: err?.stack ?? null,
  });

  logError('[q_catalog] 任务失败(兼容日志)', err);
});

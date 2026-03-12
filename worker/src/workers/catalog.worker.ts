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
      throw new Error('Missing channelIdRaw in job payload');
    }

    return handleCatalogJob(channelIdRaw);
  },
  { connection: connection as any, concurrency: 3 },
);

catalogWorker.on('completed', (job) => {
  logger.info('[q_catalog] completed job', { jobId: String(job.id) });
});

catalogWorker.on('failed', (job, err) => {
  logError('[q_catalog] failed job', {
    jobId: job?.id ? String(job.id) : null,
    error: err,
  });
});

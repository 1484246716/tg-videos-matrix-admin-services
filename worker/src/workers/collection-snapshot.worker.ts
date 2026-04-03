import { Worker } from 'bullmq';
import { connection } from '../infra/redis';
import { logger } from '../logger';
import { refreshCollectionSnapshotIncremental } from '../services/collection-snapshot.service';

export const collectionSnapshotWorker = new Worker(
  'q_collection_snapshot',
  async (job) => {
    if (job.name === 'bootstrap-check') {
      return { ok: true, skipped: true, reason: 'bootstrap-check' };
    }

    return refreshCollectionSnapshotIncremental();
  },
  { connection: connection as any, concurrency: 1 },
);

collectionSnapshotWorker.on('completed', (job) => {
  logger.info('[q_collection_snapshot] 任务完成', { jobId: String(job.id) });
});

collectionSnapshotWorker.on('failed', (job, err) => {
  logger.error('[q_collection_snapshot] 任务失败', {
    jobId: job?.id ? String(job.id) : null,
    jobName: job?.name ?? null,
    errName: err?.name ?? null,
    errMessage: err?.message ?? null,
  });
});

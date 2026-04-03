import { collectionSnapshotQueue } from '../infra/redis';

export async function scheduleCollectionSnapshotRefresh() {
  const jobId = 'collection-snapshot-incremental';
  const existingJob = await collectionSnapshotQueue.getJob(jobId);
  if (existingJob) {
    const state = await existingJob.getState();
    if (state !== 'failed') return;
    await existingJob.remove();
  }

  await collectionSnapshotQueue.add(
    'collection-snapshot-refresh',
    { triggeredAt: new Date().toISOString() },
    {
      jobId,
      removeOnComplete: true,
      removeOnFail: 200,
    },
  );
}

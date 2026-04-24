/**
 * Collection Snapshot Scheduler：定时触发合集快照增量刷新任务。
 * 在 bootstrap 定时触发，负责节流判断与投递 snapshot worker。
 */

import { COLLECTION_SNAPSHOT_REFRESH_MS } from '../config/env';
import { collectionSnapshotQueue, connection } from '../infra/redis';

// 按节流窗口入队一次合集快照增量刷新任务。
export async function scheduleCollectionSnapshotRefresh() {
  const throttleKey = 'scheduler:collection-snapshot:throttle';
  const throttleAcquired = await connection.set(
    throttleKey,
    Date.now().toString(),
    'PX',
    COLLECTION_SNAPSHOT_REFRESH_MS,
    'NX',
  );

  if (throttleAcquired !== 'OK') return;

  const jobId = 'collection-snapshot-incremental';
  try {
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
  } catch (error) {
    await connection.del(throttleKey);
    throw error;
  }
}

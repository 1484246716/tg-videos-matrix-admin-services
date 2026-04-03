import '../config/env';
import {
  catalogQueue,
  collectionSnapshotQueue,
  dispatchQueue,
  relayUploadQueue,
  massMessageQueue,
  searchIndexQueue,
} from '../infra/redis';
import { ensureWorkerPrismaModels, logWorkerDatabaseFingerprint } from '../infra/prisma';
import { logger, logError } from '../logger';
import { SCHEDULER_POLL_MS, telegramApiBase } from '../config/env';
import { scheduleEnabledTaskDefinitions } from '../scheduler/task-definition-scheduler';
import { scheduleDueMassMessageItems } from '../scheduler/mass-message-scheduler';
import { scheduleCollectionSnapshotRefresh } from '../scheduler/collection-snapshot-scheduler';
import { reconcileTypeAStuckAssets } from '../services/typea-reconcile.service';
import { auditTypeAHealth } from '../services/typea-audit.service';
import { enqueueChangedCollectionEpisodes } from '../services/search-index-trigger.service';
import { TYPEA_RECONCILE_ENABLED } from '../config/env';
import '../workers/dispatch.worker';
import '../workers/relay-upload.worker';
import '../workers/catalog.worker';
import '../workers/mass-message.worker';
import '../workers/search-index.worker';
import '../workers/collection-snapshot.worker';

async function drainStaleRelayJobs() {
  logger.info('[bootstrap] 正在清理过期的中转上传任务...');
  let removed = 0;

  const failedJobs = await relayUploadQueue.getFailed(0, 500);
  for (const job of failedJobs) {
    await job.remove();
    removed += 1;
  }

  const waitingJobs = await relayUploadQueue.getWaiting(0, 500);
  for (const job of waitingJobs) {
    if (job.name !== 'bootstrap-check') {
      await job.remove();
      removed += 1;
    }
  }

  const delayedJobs = await relayUploadQueue.getDelayed(0, 500);
  for (const job of delayedJobs) {
    await job.remove();
    removed += 1;
  }

  if (removed > 0) {
    logger.info('[bootstrap] 已清理过期的中转上传任务', { count: removed });
  }
}

export async function bootstrapWorker() {
  logWorkerDatabaseFingerprint();
  ensureWorkerPrismaModels();

  await dispatchQueue.add(
    'bootstrap-check',
    { source: 'worker_startup', timestamp: new Date().toISOString() },
    { removeOnComplete: true, removeOnFail: 100 },
  );

  await catalogQueue.add(
    'bootstrap-check',
    { source: 'worker_startup', timestamp: new Date().toISOString() },
    { removeOnComplete: true, removeOnFail: 100 },
  );

  await relayUploadQueue.add(
    'bootstrap-check',
    { source: 'worker_startup', timestamp: new Date().toISOString() },
    { removeOnComplete: true, removeOnFail: 100 },
  );

  await massMessageQueue.add(
    'bootstrap-check',
    { source: 'worker_startup', timestamp: new Date().toISOString() },
    { removeOnComplete: true, removeOnFail: 100 },
  );

  await searchIndexQueue.add(
    'bootstrap-check',
    { source: 'worker_startup', timestamp: new Date().toISOString() },
    { removeOnComplete: true, removeOnFail: 100 },
  );

  await collectionSnapshotQueue.add(
    'bootstrap-check',
    { source: 'worker_startup', timestamp: new Date().toISOString() },
    { removeOnComplete: true, removeOnFail: 100 },
  );

  await drainStaleRelayJobs();

  logger.info('[bootstrap] Telegram API 地址', { telegramApiBase });
  logger.info('[bootstrap] 调度轮询间隔', { schedulerPollMs: SCHEDULER_POLL_MS });

  setInterval(() => {
    void scheduleEnabledTaskDefinitions().catch((err) => {
      logError('[scheduler:task-definitions] 调度异常', err);
    });

    void scheduleDueMassMessageItems().catch((err) => {
      logError('[scheduler:mass-message-items] 调度异常', err);
    });

    if (TYPEA_RECONCILE_ENABLED) {
      void reconcileTypeAStuckAssets().catch((err) => {
        logError('[scheduler:typea-reconcile] 对账修复异常', err);
      });

      void auditTypeAHealth().catch((err) => {
        logError('[scheduler:typea-audit] 巡检快照异常', err);
      });
    }

    void enqueueChangedCollectionEpisodes().catch((err) => {
      logError('[scheduler:search-index-episodes] 变更扫描异常', err);
    });

    void scheduleCollectionSnapshotRefresh().catch((err) => {
      logError('[scheduler:collection-snapshot] 增量刷新调度异常', err);
    });
  }, SCHEDULER_POLL_MS);

  logger.info(
    'Worker 已启动，队列：q_dispatch + q_relay_upload + q_catalog + q_mass_message + q_search_index，任务调度已启用',
  );
}

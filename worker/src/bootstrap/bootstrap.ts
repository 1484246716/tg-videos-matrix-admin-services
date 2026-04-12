import '../config/env';
import {
  catalogQueue,
  collectionSnapshotQueue,
  dispatchQueue,
  relayUploadQueue,
  massMessageQueue,
  searchIndexQueue,
  cloneCrawlScheduleQueue,
  cloneChannelIndexQueue,
  cloneMediaDownloadQueue,
  cloneGuardWaitQueue,
  cloneRetryQueue,
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
import { TYPEA_RECONCILE_ENABLED, CLONE_DOWNLOAD_RECONCILE_ENABLED } from '../config/env';
import { auditCloneHealth } from '../clone-channels/services/clone-audit.service';
import { reconcileCloneDownloadStuck } from '../clone-channels/services/clone-download-reconcile.service';
import '../workers/dispatch.worker';
import '../workers/relay-upload.worker';
import '../workers/catalog.worker';
import '../workers/mass-message.worker';
import '../workers/search-index.worker';
import '../workers/collection-snapshot.worker';
import '../clone-channels/workers/clone-crawl-schedule.worker';
import '../clone-channels/workers/clone-channel-index.worker';
import '../clone-channels/workers/clone-video-download.worker';
import '../clone-channels/workers/clone-guard-wait.worker';
import '../clone-channels/workers/clone-retry.worker';

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

  await cloneCrawlScheduleQueue.add(
    'bootstrap-check',
    { source: 'worker_startup', timestamp: new Date().toISOString() },
    { removeOnComplete: true, removeOnFail: 100 },
  );

  await cloneChannelIndexQueue.add(
    'bootstrap-check',
    { source: 'worker_startup', timestamp: new Date().toISOString() },
    { removeOnComplete: true, removeOnFail: 100 },
  );

  await cloneMediaDownloadQueue.add(
    'bootstrap-check',
    { source: 'worker_startup', timestamp: new Date().toISOString() },
    { removeOnComplete: true, removeOnFail: 100 },
  );

  await cloneGuardWaitQueue.add(
    'bootstrap-check',
    { source: 'worker_startup', timestamp: new Date().toISOString() },
    { removeOnComplete: true, removeOnFail: 100 },
  );

  await cloneRetryQueue.add(
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

    void auditCloneHealth().catch((err) => {
      logError('[scheduler:clone-audit] 巡检快照异常', err);
    });

    if (CLONE_DOWNLOAD_RECONCILE_ENABLED) {
      void reconcileCloneDownloadStuck().catch((err) => {
        logError('[scheduler:clone-download-reconcile] 下载卡住纠偏异常', err);
      });
    }

    void cloneCrawlScheduleQueue
      .add(
        'clone-crawl-schedule-tick',
        { source: 'polling_tick', runAt: new Date().toISOString() },
        { removeOnComplete: true, removeOnFail: 100 },
      )
      .then(() => {
        logger.info('[clone][调度/Scheduler] 已入队 crawl 调度任务 / crawl schedule tick enqueued');
      })
      .catch((err) => {
        logError('[clone][调度/Scheduler] crawl 调度入队失败 / failed to enqueue crawl schedule tick', err);
      });
  }, SCHEDULER_POLL_MS);

  logger.info(
    'Worker 已启动，队列：q_dispatch + q_relay_upload + q_catalog + q_mass_message + q_search_index，任务调度已启用',
  );
}

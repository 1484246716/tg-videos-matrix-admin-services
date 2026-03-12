import '../config/env';
import { catalogQueue, dispatchQueue, relayUploadQueue, massMessageQueue } from '../infra/redis';
import { logger, logError } from '../logger';
import { telegramApiBase } from '../config/env';
import { scheduleEnabledTaskDefinitions } from '../scheduler/task-definition-scheduler';
import { scheduleDueMassMessageItems } from '../scheduler/mass-message-scheduler';
import '../workers/dispatch.worker';
import '../workers/relay-upload.worker';
import '../workers/catalog.worker';
import '../workers/mass-message.worker';

async function drainStaleRelayJobs() {
  logger.info('[bootstrap] draining stale relay upload jobs...');
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
    logger.info('[bootstrap] removed stale relay upload jobs', { count: removed });
  }
}

export async function bootstrapWorker() {
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

  await drainStaleRelayJobs();

  logger.info('[bootstrap] telegram api base', { telegramApiBase });

  setInterval(() => {
    void scheduleEnabledTaskDefinitions().catch((err) => {
      logError('[scheduler:task-definitions] error', err);
    });

    void scheduleDueMassMessageItems().catch((err) => {
      logError('[scheduler:mass-message-items] error', err);
    });
  }, 5000);

  logger.info(
    'Worker started. Queues: q_dispatch + q_relay_upload + q_catalog + q_mass_message, task-definitions scheduler enabled',
  );
}

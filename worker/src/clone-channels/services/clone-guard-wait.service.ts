import { cloneMediaDownloadQueue } from '../../infra/redis';
import { logger } from '../../logger';
import { CloneMediaDownloadJob } from '../types/clone-queue.types';
import {
  dequeueNextGuardWaitJobRoundRobin,
  enqueueGuardWaitJobByChannel,
  getGuardWaitFairnessSnapshot,
} from './clone-guard-wait-fairness.service';

let processedCount = 0;

function normalizeChannelUsername(raw: string | undefined) {
  return (raw ?? '').trim().replace(/^@+/, '').toLowerCase();
}

export async function enqueueCloneGuardWait(job: CloneMediaDownloadJob) {
  const channelUsername = normalizeChannelUsername(job.channelUsername);
  if (!channelUsername) return;

  await enqueueGuardWaitJobByChannel({
    channelUsername,
    payload: {
      ...job,
      channelUsername,
      retryCount: job.retryCount ?? 0,
    },
  });

  const snapshot = await getGuardWaitFairnessSnapshot();
  logger.info('[clone][guard-wait][metric] enqueued', {
    itemId: job.itemId,
    channelUsername,
    guardWaitChannelCount: snapshot.channelCount,
    guardWaitTopChannels: snapshot.topChannels,
  });
}

export async function processCloneGuardWait(job: CloneMediaDownloadJob) {
  await enqueueCloneGuardWait(job);

  const next = await dequeueNextGuardWaitJobRoundRobin();
  if (!next) {
    logger.info('[clone][guard-wait] no next candidate after enqueue', {
      itemId: job.itemId,
      channelUsername: normalizeChannelUsername(job.channelUsername),
    });
    return;
  }

  const itemId = String((next.payload as { itemId?: string | number | bigint }).itemId ?? '').trim();
  if (!itemId) {
    logger.warn('[clone][guard-wait] skip requeue due to missing itemId', {
      sourceItemId: job.itemId,
      pickedChannelUsername: next.channelUsername,
    });
    return;
  }

  const jobId = `clone-download-item-${itemId}`;
  const existing = await cloneMediaDownloadQueue.getJob(jobId);
  if (existing) {
    const state = await existing.getState();
    if (state === 'waiting' || state === 'active' || state === 'delayed') {
      logger.info('[clone][guard-wait] skip duplicate requeue due to existing active job', {
        itemId,
        jobId,
        state,
        pickedChannelUsername: next.channelUsername,
      });
      return;
    }
    if (state === 'failed') {
      await existing.remove();
    }
  }

  await cloneMediaDownloadQueue.add(
    'clone-media-download-from-guard-wait-rr',
    {
      ...next.payload,
      retryCount: Number((next.payload as { retryCount?: number }).retryCount ?? 0),
    },
    { jobId, removeOnComplete: true, removeOnFail: 100 },
  );

  processedCount += 1;

  if (processedCount % 20 === 0) {
    const snapshot = await getGuardWaitFairnessSnapshot();
    logger.info('[clone][guard-wait][metric] fairness snapshot', {
      processedCount,
      guardWaitChannelCount: snapshot.channelCount,
      guardWaitTopChannels: snapshot.topChannels,
    });
  }

  logger.info('[clone][guard-wait] requeued by round-robin', {
    pickedChannelUsername: next.channelUsername,
    remainingInPickedChannel: next.remaining,
    sourceItemId: job.itemId,
  });
}

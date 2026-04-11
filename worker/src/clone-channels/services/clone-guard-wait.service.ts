import { cloneVideoDownloadQueue } from '../../infra/redis';
import { logger } from '../../logger';
import { CloneVideoDownloadJob } from '../types/clone-queue.types';
import {
  dequeueNextGuardWaitJobRoundRobin,
  enqueueGuardWaitJobByChannel,
  getGuardWaitFairnessSnapshot,
} from './clone-guard-wait-fairness.service';

let processedCount = 0;

function normalizeChannelUsername(raw: string | undefined) {
  return (raw ?? '').trim().replace(/^@+/, '').toLowerCase();
}

export async function enqueueCloneGuardWait(job: CloneVideoDownloadJob) {
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

export async function processCloneGuardWait(job: CloneVideoDownloadJob) {
  await enqueueCloneGuardWait(job);

  const next = await dequeueNextGuardWaitJobRoundRobin();
  if (!next) {
    logger.info('[clone][guard-wait] no next candidate after enqueue', {
      itemId: job.itemId,
      channelUsername: normalizeChannelUsername(job.channelUsername),
    });
    return;
  }

  await cloneVideoDownloadQueue.add(
    'clone-video-download-from-guard-wait-rr',
    {
      ...next.payload,
      retryCount: Number((next.payload as { retryCount?: number }).retryCount ?? 0),
    },
    { removeOnComplete: true, removeOnFail: 100 },
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

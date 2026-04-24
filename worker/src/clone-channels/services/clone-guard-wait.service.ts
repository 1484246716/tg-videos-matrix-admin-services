/**
 * Clone Channels guard-wait 服务：缓存受限任务并按公平策略恢复下载。
 * 用于在 clone 调度/执行链路中将被风控暂停的任务按频道轮询重入队。
 */

import { cloneMediaDownloadQueue } from '../../infra/redis';
import { prisma } from '../../infra/prisma';
import { logger } from '../../logger';
import { CloneMediaDownloadJob } from '../types/clone-queue.types';
import {
  dequeueNextGuardWaitJobRoundRobin,
  enqueueGuardWaitJobByChannel,
  getGuardWaitFairnessSnapshot,
} from './clone-guard-wait-fairness.service';
import { prepareCloneDownloadJobForEnqueue } from './clone-download-queue.service';

let processedCount = 0;

// 规范化频道用户名：去除 @ 前缀并统一小写。
function normalizeChannelUsername(raw: string | undefined) {
  return (raw ?? '').trim().replace(/^@+/, '').toLowerCase();
}

// 将任务按频道写入 guard-wait 公平队列。
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

// 处理 guard-wait 任务：入公平队列并轮询取下一个可恢复任务重入下载队列。
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

  const prepared = await prepareCloneDownloadJobForEnqueue({
    itemId,
    source: 'guard-wait-round-robin',
  });

  if (!prepared.canEnqueue) {
    logger.info('[clone][guard-wait] skip duplicate requeue due to existing job state', {
      itemId,
      jobId: prepared.jobId,
      state: prepared.existingState,
      reason: prepared.reason,
      pickedChannelUsername: next.channelUsername,
    });
    return;
  }

  const itemIdBigInt = BigInt(itemId);
  const transitioned = await prisma.cloneCrawlItem.updateMany({
    where: {
      id: itemIdBigInt,
      downloadStatus: {
        in: ['paused_by_guard', 'queued', 'failed_retryable', 'none'],
      } as any,
    },
    data: {
      downloadStatus: 'queued',
      downloadLeaseUntil: null,
      downloadHeartbeatAt: null,
      downloadWorkerJobId: null,
      downloadErrorCode: null,
      downloadError: null,
    } as any,
  });

  if (transitioned.count === 0) {
    const latest = await prisma.cloneCrawlItem.findUnique({
      where: { id: itemIdBigInt },
      select: {
        downloadStatus: true,
        localPath: true,
      },
    });

    logger.info('[clone][guard-wait] skip requeue due to state transition guard', {
      itemId,
      jobId: prepared.jobId,
      pickedChannelUsername: next.channelUsername,
      currentStatus: latest?.downloadStatus ?? null,
      localPath: latest?.localPath ?? null,
    });
    return;
  }

  await cloneMediaDownloadQueue.add(
    'clone-media-download-from-guard-wait-rr',
    {
      ...next.payload,
      retryCount: Number((next.payload as { retryCount?: number }).retryCount ?? 0),
    },
    { jobId: prepared.jobId, removeOnComplete: true, removeOnFail: 100 },
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
    itemId,
    jobId: prepared.jobId,
    pickedChannelUsername: next.channelUsername,
    remainingInPickedChannel: next.remaining,
    sourceItemId: job.itemId,
  });
}

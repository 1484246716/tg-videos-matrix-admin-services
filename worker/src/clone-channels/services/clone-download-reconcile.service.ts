import { stat } from 'node:fs/promises';
import { cloneMediaDownloadQueue } from '../../infra/redis';
import { prisma } from '../../infra/prisma';
import { logger } from '../../logger';
import {
  CLONE_DOWNLOAD_RECOVER_MAX,
  CLONE_DOWNLOAD_RECONCILE_BATCH,
  CLONE_DOWNLOAD_STUCK_MS,
} from '../../config/env';

async function isActiveQueueStateByJobId(jobId: string | null | undefined) {
  if (!jobId) return false;
  const job = await cloneMediaDownloadQueue.getJob(jobId);
  if (!job) return false;
  const state = await job.getState();
  return state === 'active' || state === 'waiting' || state === 'delayed';
}

async function isDownloadedFileReady(localPath: string | null | undefined) {
  if (!localPath) return false;
  try {
    const s = await stat(localPath);
    return s.isFile() && s.size > 0;
  } catch {
    return false;
  }
}

function buildRequeuePayload(item: any) {
  return {
    taskId: item.taskId.toString(),
    runId: item.runId.toString(),
    itemId: item.id.toString(),
    channelUsername: item.channelUsername,
    groupedId: item.groupedId ?? undefined,
    groupKey: item.groupKey ?? undefined,
    expectedFileSize: item.fileSize ? item.fileSize.toString() : undefined,
    expectedMimeType: item.mimeType ?? undefined,
    targetPath: item.localPath ?? undefined,
    retryCount: item.retryCount ?? 0,
  };
}

export async function reconcileCloneDownloadStuck() {
  const now = new Date();
  const staleBefore = new Date(Date.now() - CLONE_DOWNLOAD_STUCK_MS);

  const items = await prisma.cloneCrawlItem.findMany({
    where: {
      OR: [
        {
          downloadStatus: 'downloading',
          downloadLeaseUntil: { lt: now } as any,
        } as any,
        {
          downloadStatus: 'queued',
          updatedAt: { lte: staleBefore },
        },
        {
          downloadStatus: 'failed_retryable',
          updatedAt: { lte: staleBefore },
        },
      ],
    },
    orderBy: { updatedAt: 'asc' },
    take: CLONE_DOWNLOAD_RECONCILE_BATCH,
  });

  let detected = 0;
  let requeued = 0;
  let leaseExpired = 0;
  let exhausted = 0;
  let skippedActive = 0;
  let patchedDownloaded = 0;

  for (const item of items as any[]) {
    detected += 1;

    const queueActive = await isActiveQueueStateByJobId(item.downloadWorkerJobId ?? null);
    if (queueActive) {
      skippedActive += 1;
      continue;
    }

    const fileReady = await isDownloadedFileReady(item.localPath ?? null);
    if (fileReady && item.downloadStatus !== 'downloaded') {
      await prisma.cloneCrawlItem.updateMany({
        where: { id: item.id },
        data: {
          downloadStatus: 'downloaded',
          downloadLeaseUntil: null,
          downloadHeartbeatAt: null,
          downloadWorkerJobId: null,
          downloadErrorCode: null,
          downloadError: null,
        } as any,
      });
      patchedDownloaded += 1;
      continue;
    }

    const recoverCount = Number(item.downloadRecoverCount ?? 0);
    if (recoverCount >= CLONE_DOWNLOAD_RECOVER_MAX) {
      await prisma.cloneCrawlItem.updateMany({
        where: { id: item.id },
        data: {
          downloadStatus: 'failed_final',
          downloadErrorCode: 'recover_exhausted',
          downloadError: `auto recover exhausted(${recoverCount})`,
          downloadLeaseUntil: null,
          downloadHeartbeatAt: null,
          downloadWorkerJobId: null,
        } as any,
      });
      exhausted += 1;
      continue;
    }

    const payload = buildRequeuePayload(item);

    await cloneMediaDownloadQueue.add('clone-media-download-reconcile', payload, {
      jobId: `clone-download-item-${item.id.toString()}`,
      removeOnComplete: true,
      removeOnFail: 100,
    });

    await prisma.cloneCrawlItem.updateMany({
      where: { id: item.id },
      data: {
        downloadStatus: 'queued',
        downloadRecoverCount: { increment: 1 },
        downloadLeaseUntil: null,
        downloadHeartbeatAt: null,
        downloadWorkerJobId: null,
        downloadErrorCode: null,
        downloadError: null,
      } as any,
    });

    if (item.downloadLeaseUntil && new Date(item.downloadLeaseUntil).getTime() < Date.now()) {
      leaseExpired += 1;
    }

    requeued += 1;
  }

  logger.info('[clone_reconcile] download stuck reconcile tick', {
    clone_download_stuck_detected_total: detected,
    clone_download_recovered_total: requeued,
    clone_download_lease_expired_total: leaseExpired,
    clone_download_recover_exhausted_total: exhausted,
    clone_download_skipped_active_total: skippedActive,
    clone_download_patched_downloaded_total: patchedDownloaded,
    scanned: items.length,
  });
}

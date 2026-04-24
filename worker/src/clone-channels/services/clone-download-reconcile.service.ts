/**
 * Clone Channels 下载自愈服务：识别卡住下载并自动修复/重入队。
 * 用于在 clone 调度/执行链路中处理租约过期、任务丢失和恢复次数耗尽。
 */

import { stat } from 'node:fs/promises';
import { cloneMediaDownloadQueue } from '../../infra/redis';
import { prisma } from '../../infra/prisma';
import { logger } from '../../logger';
import {
  CLONE_DOWNLOAD_RECOVER_MAX,
  CLONE_DOWNLOAD_RECONCILE_BATCH,
  CLONE_GUARD_WAIT_STUCK_MS,
  CLONE_DOWNLOAD_STUCK_MS,
} from '../../config/env';
import {
  hasCloneDownloadJobInFlight,
  prepareCloneDownloadJobForEnqueue,
} from './clone-download-queue.service';

// 判断本地下载文件是否可用（存在且大小大于 0）。
async function isDownloadedFileReady(localPath: string | null | undefined) {
  if (!localPath) return false;
  try {
    const s = await stat(localPath);
    return s.isFile() && s.size > 0;
  } catch {
    return false;
  }
}

// 组装重入队 payload，保留下载任务恢复所需上下文。
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

// 扫描并修复卡住下载：补状态、判在途、重入队或落终态。
export async function reconcileCloneDownloadStuck() {
  const now = new Date();
  const staleBefore = new Date(Date.now() - CLONE_DOWNLOAD_STUCK_MS);
  const guardWaitStaleBefore = new Date(Date.now() - CLONE_GUARD_WAIT_STUCK_MS);

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
        {
          downloadStatus: 'paused_by_guard',
          updatedAt: { lte: guardWaitStaleBefore },
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

    const queueActive = await hasCloneDownloadJobInFlight(item.id);
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
    const prepared = await prepareCloneDownloadJobForEnqueue({
      itemId: item.id,
      source: 'reconcile',
    });

    if (!prepared.canEnqueue) {
      skippedActive += 1;
      continue;
    }

    await cloneMediaDownloadQueue.add('clone-media-download-reconcile', payload, {
      jobId: prepared.jobId,
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

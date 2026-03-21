import { MediaStatus } from '@prisma/client';
import { access } from 'node:fs/promises';
import {
  TYPEA_RECONCILE_BATCH,
  TYPEA_INGEST_STALE_MS,
} from '../config/env';
import { prisma } from '../infra/prisma';
import { relayUploadQueue } from '../infra/redis';
import { logger } from '../logger';
import { TYPEA_INGEST_ERROR_CODE, TYPEA_INGEST_FINAL_REASON } from '../shared/metrics';

async function pathExists(filePath: string | null | undefined) {
  if (!filePath) return false;
  try {
    await access(filePath);
    return true;
  } catch {
    return false;
  }
}

export async function reconcileTypeAStuckAssets() {
  const staleBefore = new Date(Date.now() - TYPEA_INGEST_STALE_MS);

  const assets = await prisma.mediaAsset.findMany({
    where: {
      OR: [
        {
          status: MediaStatus.ingesting,
          updatedAt: { lte: staleBefore },
          telegramFileId: null,
        },
        {
          status: { in: [MediaStatus.ready, MediaStatus.failed, MediaStatus.ingesting] },
          telegramFileId: null,
        },
      ],
    },
    orderBy: { updatedAt: 'asc' },
    take: TYPEA_RECONCILE_BATCH,
    select: {
      id: true,
      status: true,
      localPath: true,
      archivePath: true,
      sourceMeta: true,
    },
  });

  let reconciled = 0;
  let missingFinal = 0;
  let staleRecovered = 0;

  for (const asset of assets) {
    const sourceMeta =
      asset.sourceMeta && typeof asset.sourceMeta === 'object'
        ? (asset.sourceMeta as Record<string, unknown>)
        : {};

    const leaseUntilRaw = sourceMeta.ingestLeaseUntil;
    const leaseUntilMs =
      typeof leaseUntilRaw === 'string' ? new Date(leaseUntilRaw).getTime() : 0;
    const leaseExpired = !leaseUntilMs || leaseUntilMs < Date.now();

    const localExists = await pathExists(asset.localPath);
    if (localExists) {
      if (asset.status === MediaStatus.ingesting && leaseExpired) {
        const jobId =
          typeof sourceMeta.ingestWorkerJobId === 'string'
            ? sourceMeta.ingestWorkerJobId
            : null;

        if (jobId) {
          const job = await relayUploadQueue.getJob(jobId);
          if (job) {
            const state = await job.getState();
            if (state === 'active' || state === 'waiting' || state === 'delayed') {
              continue;
            }
          }
        }

        await prisma.mediaAsset.update({
          where: { id: asset.id },
          data: {
            status: MediaStatus.ready,
            ingestError: null,
            sourceMeta: {
              ...sourceMeta,
              ingestLeaseUntil: null,
              ingestWorkerJobId: null,
              ingestRecoveredAt: new Date().toISOString(),
              ingestRecoveredReason: 'LEASE_EXPIRED_AND_JOB_NOT_ACTIVE',
            },
          },
        });
        staleRecovered += 1;
      }
      continue;
    }

    const archiveExists = await pathExists(asset.archivePath);
    if (archiveExists && asset.archivePath) {
      await prisma.mediaAsset.update({
        where: { id: asset.id },
        data: {
          localPath: asset.archivePath,
          ingestError: null,
          status: asset.status === MediaStatus.ingesting ? MediaStatus.ready : asset.status,
          sourceMeta: {
            ...sourceMeta,
            ingestReconciledAt: new Date().toISOString(),
            ingestRecoveredReason: 'LOCAL_MISSING_ARCHIVE_EXISTS',
          },
        },
      });
      reconciled += 1;
      continue;
    }

    await prisma.mediaAsset.update({
      where: { id: asset.id },
      data: {
        status: MediaStatus.failed,
        ingestError: 'SRC_FILE_MISSING_FINAL: source file missing and unrecoverable',
        sourceMeta: {
          ...sourceMeta,
          ingestErrorCode: TYPEA_INGEST_ERROR_CODE.srcFileMissing,
          ingestFinalReason: TYPEA_INGEST_FINAL_REASON.failedFinal,
          ingestLeaseUntil: null,
          ingestWorkerJobId: null,
          ingestLastHeartbeatAt: new Date().toISOString(),
        },
      },
    });

    missingFinal += 1;
  }

  logger.info('[typea_metrics] reconcile tick', {
    typea_reconciled_total: reconciled,
    typea_file_missing_total: missingFinal,
    typea_ingesting_stale_total: staleRecovered,
    task_run_total: assets.length,
    task_failed_total: 0,
    task_dead_total: missingFinal,
    scanned: assets.length,
    metric_labels: {
      typea_reconciled_total: 'TypeA 对账修复总数',
      typea_file_missing_total: 'TypeA 源文件缺失终态总数',
      typea_ingesting_stale_total: 'TypeA ingesting 超时回收总数',
    },
  });
}

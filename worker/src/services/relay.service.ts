import { stat, unlink } from 'node:fs/promises';
import { basename } from 'node:path';
import { MediaStatus } from '@prisma/client';
import { TYPEA_MAX_UPLOAD_SIZE_MB } from '../config/env';
import { prisma, getTaskDefinitionModel } from '../infra/prisma';
import { logger } from '../logger';
import { hashFile, scanChannelVideos, waitForFileStable } from '../shared/file-utils';
import { tryAcquireRelayPathLock, releaseRelayPathLock } from '../shared/relay-path-lock';
import { TYPEA_INGEST_ERROR_CODE, TYPEA_INGEST_FINAL_REASON } from '../shared/metrics';

async function removeLocalDuplicateFile(filePath: string, mediaAssetId: string) {
  try {
    await unlink(filePath);
    logger.warn('[relay] 检测到重复上传资源，已删除本地文件避免重复扫描', {
      filePath,
      mediaAssetId,
      action: 'delete_local_duplicate_file',
    });
  } catch (error) {
    logger.warn('[relay] 检测到重复上传资源，但删除本地文件失败', {
      filePath,
      mediaAssetId,
      error: error instanceof Error ? error.message : String(error),
      action: 'delete_local_duplicate_file_failed',
    });
  }
}

export async function enqueueRelayAssetsFromTaskDefinition(taskDefinitionId: bigint) {
  const definition = await getTaskDefinitionModel().findUnique({
    where: { id: taskDefinitionId },
    select: {
      id: true,
      relayChannelId: true,
      priority: true,
      maxRetries: true,
      payload: true,
    },
  });

  if (!definition || !definition.relayChannelId) {
    return {
      scannedFiles: 0,
      createdAssets: 0,
      enqueuedTasks: 0,
      skipped: true,
      reason: 'relayChannelId is missing',
    };
  }

  const payload = (definition.payload ?? {}) as Record<string, unknown>;
  const payloadChannelIds = Array.isArray(payload.channelIds)
    ? payload.channelIds.map((v) => String(v))
    : [];

  const channels = await prisma.channel.findMany({
    where: {
      status: 'active',
      ...(payloadChannelIds.length > 0
        ? { id: { in: payloadChannelIds.map((id) => BigInt(id)) } }
        : {}),
    },
    select: { id: true, folderPath: true },
  });

  const maxUploadSizeBytes = TYPEA_MAX_UPLOAD_SIZE_MB * 1024 * 1024;

  let scannedFiles = 0;
  let createdAssets = 0;
  let enqueuedTasks = 0;
  let rejectedTooLarge = 0;

  for (const channel of channels) {
    let files: string[] = [];
    try {
      files = await scanChannelVideos(channel.folderPath);
    } catch {
      continue;
    }

    for (const filePath of files) {
      scannedFiles += 1;

      const pathLock = await tryAcquireRelayPathLock(filePath);
      if (!pathLock.acquired) {
        logger.info('[relay] 扫描跳过：localPath 锁被占用', {
          filePath,
          lockKey: pathLock.lockKey,
          reason: 'path_lock_busy_skip',
        });
        continue;
      }

      try {
        await waitForFileStable(filePath);
      } catch (error) {
        logger.warn('[relay] 文件未稳定或不可用，跳过', {
          filePath,
          reason: error instanceof Error ? error.message : String(error),
        });
        await releaseRelayPathLock(pathLock);
        continue;
      }

      try {
        const s = await stat(filePath);

      const existedByPath = await prisma.mediaAsset.findFirst({
        where: {
          localPath: filePath,
          status: { in: [MediaStatus.ready, MediaStatus.ingesting, MediaStatus.relay_uploaded] },
        },
        select: { id: true, status: true },
      });

      if (existedByPath) {
        logger.info('[relay] 扫描跳过：localPath 已存在活跃记录', {
          filePath,
          existedMediaAssetId: existedByPath.id.toString(),
          existedStatus: existedByPath.status,
          reason: 'path_dedup_skip',
        });
        continue;
      }

      if (s.size > maxUploadSizeBytes) {
        const ingestError = `FILE_TOO_LARGE: file size ${s.size} exceeds ${maxUploadSizeBytes} bytes (${TYPEA_MAX_UPLOAD_SIZE_MB}MB policy)`;

        const fileHash = await hashFile(filePath);
        const existed = await prisma.mediaAsset.findUnique({
          where: {
            fileHash_fileSize: {
              fileHash,
              fileSize: s.size,
            },
          },
          select: { id: true, sourceMeta: true },
        });

        const sourceMeta =
          existed?.sourceMeta && typeof existed.sourceMeta === 'object'
            ? (existed.sourceMeta as Record<string, unknown>)
            : {};

        if (existed) {
          await prisma.mediaAsset.update({
            where: { id: existed.id },
            data: {
              status: MediaStatus.failed,
              ingestError,
              sourceMeta: {
                ...sourceMeta,
                relayChannelId: definition.relayChannelId.toString(),
                taskDefinitionId: definition.id.toString(),
                ingestErrorCode: TYPEA_INGEST_ERROR_CODE.fileTooLarge,
                ingestFinalReason: TYPEA_INGEST_FINAL_REASON.fileTooLarge,
                ingestLeaseUntil: null,
                ingestWorkerJobId: null,
                ingestRejectedAt: new Date().toISOString(),
              },
            },
          });
        } else {
          await prisma.mediaAsset.create({
            data: {
              channelId: channel.id,
              originalName: basename(filePath),
              localPath: filePath,
              fileSize: BigInt(s.size),
              fileHash,
              status: MediaStatus.failed,
              ingestError,
              sourceMeta: {
                relayChannelId: definition.relayChannelId.toString(),
                taskDefinitionId: definition.id.toString(),
                ingestErrorCode: TYPEA_INGEST_ERROR_CODE.fileTooLarge,
                ingestFinalReason: TYPEA_INGEST_FINAL_REASON.fileTooLarge,
                ingestRejectedAt: new Date().toISOString(),
                relayPriority: definition.priority,
                relayMaxRetries: definition.maxRetries,
              },
            },
          });
          createdAssets += 1;
        }

        rejectedTooLarge += 1;
        logger.warn('[typea_metrics] relay scan rejected too large file', {
          typea_rejected_too_large_total: 1,
          filePath,
          fileSize: s.size,
          maxUploadSizeBytes,
          maxUploadSizeMb: TYPEA_MAX_UPLOAD_SIZE_MB,
          metric_labels: {
            typea_rejected_too_large_total: 'TypeA 超大小文件拒绝总数',
          },
        });
        continue;
      }

      const hashStart = Date.now();
      const fileHash = await hashFile(filePath);
      const hashDurationMs = Date.now() - hashStart;

      if (hashDurationMs > 500) {
        logger.info('[relay] hashFile 耗时', {
          stage: 'hash_file',
          filePath,
          fileSize: s.size,
          durationMs: hashDurationMs,
        });
      }

      let asset = await prisma.mediaAsset.findUnique({
        where: {
          fileHash_fileSize: {
            fileHash,
            fileSize: s.size,
          },
        },
        select: {
          id: true,
          status: true,
          sourceMeta: true,
          telegramFileId: true,
          relayMessageId: true,
        },
      });

      if (!asset) {
        asset = await prisma.mediaAsset.create({
          data: {
            channelId: channel.id,
            originalName: basename(filePath),
            localPath: filePath,
            fileSize: BigInt(s.size),
            fileHash,
            status: MediaStatus.ready,
            sourceMeta: {
              relayChannelId: definition.relayChannelId.toString(),
              taskDefinitionId: definition.id.toString(),
              relayEnqueueAt: new Date().toISOString(),
              relayPriority: definition.priority,
              relayMaxRetries: definition.maxRetries,
              ingestRetryCount: 0,
            },
          },
          select: {
            id: true,
            status: true,
            sourceMeta: true,
            telegramFileId: true,
            relayMessageId: true,
          },
        });
        createdAssets += 1;
      }

      if (!asset) {
        continue;
      }

      const sourceMeta =
        asset.sourceMeta && typeof asset.sourceMeta === 'object'
          ? (asset.sourceMeta as Record<string, unknown>)
          : {};
      const ingestFinalReason =
        typeof sourceMeta.ingestFinalReason === 'string'
          ? sourceMeta.ingestFinalReason
          : '';
      const isRetryableFailed =
        asset.status === MediaStatus.failed &&
        ingestFinalReason === TYPEA_INGEST_FINAL_REASON.retryable;

      const isAlreadyUploadedDuplicate =
        asset.status === MediaStatus.relay_uploaded ||
        Boolean(asset.telegramFileId) ||
        Boolean(asset.relayMessageId);

      if (isAlreadyUploadedDuplicate) {
        const duplicateName = basename(filePath);
        const duplicateError = `DUPLICATE_ALREADY_UPLOADED: 本地重复文件已跳过 ${duplicateName}`;

        const existingDuplicateFailed = await prisma.mediaAsset.findFirst({
          where: {
            localPath: filePath,
            status: MediaStatus.failed,
            sourceMeta: {
              path: ['ingestFinalReason'],
              equals: 'duplicate_already_uploaded',
            },
          },
          select: { id: true, sourceMeta: true },
        });

        if (existingDuplicateFailed) {
          const failedMeta =
            existingDuplicateFailed.sourceMeta && typeof existingDuplicateFailed.sourceMeta === 'object'
              ? (existingDuplicateFailed.sourceMeta as Record<string, unknown>)
              : {};

          await prisma.mediaAsset.update({
            where: { id: existingDuplicateFailed.id },
            data: {
              ingestError: duplicateError,
              sourceMeta: {
                ...failedMeta,
                relayChannelId: definition.relayChannelId.toString(),
                taskDefinitionId: definition.id.toString(),
                ingestFinalReason: 'duplicate_already_uploaded',
                duplicateDetectedAt: new Date().toISOString(),
                duplicateLocalPath: filePath,
                duplicateOfMediaAssetId: asset.id.toString(),
              },
            },
          });
        } else {
          const dedupFileHash = `${fileHash}:dup:${Date.now().toString(36)}`;
          await prisma.mediaAsset.create({
            data: {
              channelId: channel.id,
              originalName: duplicateName,
              localPath: filePath,
              fileSize: BigInt(s.size),
              fileHash: dedupFileHash,
              status: MediaStatus.failed,
              ingestError: duplicateError,
              sourceMeta: {
                relayChannelId: definition.relayChannelId.toString(),
                taskDefinitionId: definition.id.toString(),
                ingestErrorCode: 'DUPLICATE_ALREADY_UPLOADED',
                ingestFinalReason: 'duplicate_already_uploaded',
                duplicateDetectedAt: new Date().toISOString(),
                duplicateLocalPath: filePath,
                duplicateOfMediaAssetId: asset.id.toString(),
              },
            },
          });
        }

        await removeLocalDuplicateFile(filePath, asset.id.toString());
        continue;
      }

      if (
        asset.status === MediaStatus.ingesting ||
        (asset.status === MediaStatus.failed && !isRetryableFailed)
      ) {
        continue;
      }

      if (isRetryableFailed) {
        logger.info('[typea_metrics] relay scan recovered retryable failed asset', {
          mediaAssetId: asset.id.toString(),
          typea_recovered_retryable_failed_total: 1,
          metric_labels: {
            typea_recovered_retryable_failed_total: 'TypeA 扫描阶段恢复可重试失败任务总数',
          },
        });
      }

      await prisma.mediaAsset.update({
        where: { id: asset.id },
        data: {
          status: MediaStatus.ready,
          ingestError: null,
          sourceMeta: {
            ...sourceMeta,
            relayChannelId: definition.relayChannelId.toString(),
            taskDefinitionId: definition.id.toString(),
            relayEnqueueAt: new Date().toISOString(),
            relayPriority: definition.priority,
            relayMaxRetries: definition.maxRetries,
          },
        },
      });

      enqueuedTasks += 1;
      } finally {
        await releaseRelayPathLock(pathLock);
      }
    }
  }

  return {
    scannedFiles,
    createdAssets,
    enqueuedTasks,
    rejectedTooLarge,
    relayChannelId: definition.relayChannelId.toString(),
  };
}

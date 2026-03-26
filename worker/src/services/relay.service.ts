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

function parseCollectionMeta(filePath: string) {
  const normalizedFile = filePath.replace(/\\/g, '/');
  const marker = '/collection/';
  const lowerFile = normalizedFile.toLowerCase();
  const markerIdx = lowerFile.indexOf(marker);
  if (markerIdx === -1) return null;

  const rest = normalizedFile.slice(markerIdx + marker.length);
  const parts = rest.split('/').filter(Boolean);
  if (parts.length < 2) return null;

  const collectionName = parts[0];
  const fileName = parts[parts.length - 1];

  const patterns = [
    /\[第\s*(\d+)\s*集\]/,
    /第\s*(\d+)\s*集/,
    /S\d+E(\d+)/i,
  ];

  let episodeNo: number | null = null;
  for (const pattern of patterns) {
    const match = fileName.match(pattern);
    if (match && match[1]) {
      const parsed = Number(match[1]);
      if (Number.isFinite(parsed)) {
        episodeNo = parsed;
        break;
      }
    }
  }

  const episodeParseFailed = episodeNo === null;
  const orderKey = `${collectionName}#${episodeNo ? String(episodeNo).padStart(4, '0') : '0000'}`;

  return {
    isCollection: true,
    collectionName,
    episodeNo,
    episodeParseFailed,
    collectionPath: `Collection/${collectionName}`,
    collectionOrderKey: orderKey,
  };
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
      const collectionMeta = parseCollectionMeta(filePath);

      if (collectionMeta?.episodeParseFailed) {
        logger.warn('[relay] 合集集号解析失败，等待重命名后重扫', {
          channelId: channel.id.toString(),
          filePath,
          collectionName: collectionMeta.collectionName,
        });
      }

      if (collectionMeta && !collectionMeta.episodeParseFailed && collectionMeta.episodeNo !== null) {
        const sameEpisodeAssets = await prisma.mediaAsset.findMany({
          where: {
            channelId: channel.id,
            sourceMeta: {
              path: ['collectionName'],
              equals: collectionMeta.collectionName,
            },
          },
          select: {
            id: true,
            localPath: true,
            sourceMeta: true,
            status: true,
          },
        });

        const conflictAssets = sameEpisodeAssets.filter((asset) => {
          if (asset.localPath === filePath) return false;
          const meta =
            asset.sourceMeta && typeof asset.sourceMeta === 'object'
              ? (asset.sourceMeta as Record<string, unknown>)
              : null;
          if (!meta || meta.isCollection !== true) return false;
          const epNo =
            typeof meta.episodeNo === 'number'
              ? meta.episodeNo
              : typeof meta.episodeNo === 'string' && /^\d+$/.test(meta.episodeNo)
                ? Number(meta.episodeNo)
                : null;
          return epNo === collectionMeta.episodeNo;
        });

        if (conflictAssets.length > 0) {
          const ingestError = `COLLECTION_EPISODE_CONFLICT: ${collectionMeta.collectionName} 第${collectionMeta.episodeNo}集重复`; 

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

          const conflictIds = conflictAssets.map((asset) => asset.id.toString());

          if (existed) {
            const sourceMetaExisted =
              existed.sourceMeta && typeof existed.sourceMeta === 'object'
                ? (existed.sourceMeta as Record<string, unknown>)
                : {};

            await prisma.mediaAsset.update({
              where: { id: existed.id },
              data: {
                status: MediaStatus.failed,
                ingestError,
                sourceMeta: {
                  ...sourceMetaExisted,
                  ...(collectionMeta ?? {}),
                  episodeConflict: true,
                  episodeConflictPolicy: 'block',
                  episodeConflictAssetIds: conflictIds,
                  ingestFinalReason: 'collection_episode_conflict',
                  ingestErrorCode: 'COLLECTION_EPISODE_CONFLICT',
                  relayChannelId: definition.relayChannelId.toString(),
                  taskDefinitionId: definition.id.toString(),
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
                  ...(collectionMeta ?? {}),
                  episodeConflict: true,
                  episodeConflictPolicy: 'block',
                  episodeConflictAssetIds: conflictIds,
                  ingestFinalReason: 'collection_episode_conflict',
                  ingestErrorCode: 'COLLECTION_EPISODE_CONFLICT',
                  relayChannelId: definition.relayChannelId.toString(),
                  taskDefinitionId: definition.id.toString(),
                  relayPriority: definition.priority,
                  relayMaxRetries: definition.maxRetries,
                },
              },
            });
            createdAssets += 1;
          }

          logger.warn('[relay] 合集重复集号冲突，按 block 策略阻塞派发', {
            channelId: channel.id.toString(),
            filePath,
            collectionName: collectionMeta.collectionName,
            episodeNo: collectionMeta.episodeNo,
            conflictAssetIds: conflictIds,
          });

          continue;
        }
      }

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
              ...(collectionMeta ?? {}),
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
            ...(collectionMeta ?? {}),
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

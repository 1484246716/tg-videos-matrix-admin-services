import { stat, unlink } from 'node:fs/promises';
import { basename } from 'node:path';
import { MediaStatus } from '@prisma/client';
import { TYPEA_MAX_UPLOAD_SIZE_MB } from '../config/env';
import { prisma, getTaskDefinitionModel } from '../infra/prisma';
import { logger } from '../logger';
import {
  buildRelayPathFingerprint,
  hashFile,
  scanChannelVideos,
  waitForFileStable,
} from '../shared/file-utils';
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

function parseGroupedMeta(filePath: string) {
  const normalized = filePath.replace(/\\/g, '/');
  const match = normalized.match(/\/(grouped-\d+|single-\d+)\//i);
  if (!match || !match[1]) return null;
  const groupKey = match[1];
  const groupedId = groupKey.toLowerCase().startsWith('grouped-')
    ? groupKey.slice('grouped-'.length)
    : null;
  return { groupKey, groupedId };
}

async function resolveSourceExpectedCount(args: {
  channelId: bigint;
  filePath: string;
  groupedMeta: { groupKey: string; groupedId: string | null } | null;
  cloneSourceMeta: { isCloneCrawlAsset: true; sourceChannelUsername: string; sourceMessageId: string } | null;
}) {
  if (!args.groupedMeta?.groupKey) return null;
  if (!args.cloneSourceMeta?.sourceChannelUsername || !args.cloneSourceMeta?.sourceMessageId) return null;

  const normalized = args.filePath.replace(/\\/g, '/');
  const marker = `/${args.groupedMeta.groupKey}/`;
  const idx = normalized.toLowerCase().lastIndexOf(marker.toLowerCase());
  if (idx === -1) return null;


  const groupedSourceChannel = args.cloneSourceMeta.sourceChannelUsername.trim().replace(/^@+/, '').toLowerCase();
  if (!groupedSourceChannel) return null;

  const groupedItemWhere = args.groupedMeta.groupedId
    ? {
        channelUsername: groupedSourceChannel,
        groupedId: args.groupedMeta.groupedId,
        OR: [
          { hasVideo: true },
          {
            mimeType: {
              startsWith: 'image/',
            },
          },
        ],
      }
    : {
        channelUsername: groupedSourceChannel,
        groupKey: args.groupedMeta.groupKey,
        OR: [
          { hasVideo: true },
          {
            mimeType: {
              startsWith: 'image/',
            },
          },
        ],
      };

  const groupedItems = await prisma.cloneCrawlItem.findMany({
    where: groupedItemWhere,
    select: {
      messageId: true,
    },
  });

  const terminalCount = groupedItems.length;
  if (terminalCount <= 0) return null;

  const sourceMessageIdBigInt = BigInt(args.cloneSourceMeta.sourceMessageId);
  const sourceMessageInGroup = groupedItems.some((item) => item.messageId === sourceMessageIdBigInt);
  if (!sourceMessageInGroup) {
    logger.warn('[typeb_group] sourceExpectedCount guard blocked: source message not in grouped terminal items', {
      channelId: args.channelId.toString(),
      filePath: args.filePath,
      groupKey: args.groupedMeta.groupKey,
      groupedId: args.groupedMeta.groupedId,
      sourceChannelUsername: groupedSourceChannel,
      sourceMessageId: args.cloneSourceMeta.sourceMessageId,
      terminalCount,
      blockedReason: 'source_message_not_in_group',
    });
    return null;
  }

  return terminalCount;
}

function parseCloneSourceMeta(filePath: string, groupedMeta: { groupKey: string; groupedId: string | null } | null) {
  if (!groupedMeta) return null;

  const fileName = basename(filePath);
  const ext = fileName.includes('.') ? fileName.slice(fileName.lastIndexOf('.')) : '';
  const stem = ext ? fileName.slice(0, -ext.length) : fileName;
  const match = stem.match(/^(.*)-(\d+)$/);
  if (!match || !match[1] || !match[2]) return null;

  const sourceChannelUsername = match[1].trim();
  const sourceMessageId = match[2].trim();
  if (!sourceChannelUsername || !/^\d+$/.test(sourceMessageId)) return null;

  return {
    isCloneCrawlAsset: true as const,
    sourceChannelUsername,
    sourceMessageId,
  };
}

function buildStoredFileHash(fileHash: string, cloneSourceMeta: { sourceMessageId: string } | null) {
  if (!cloneSourceMeta?.sourceMessageId) return fileHash;
  return `${fileHash}:srcmsg:${cloneSourceMeta.sourceMessageId}`;
}

function shouldSkipRelayScanFile(filePath: string): { skip: boolean; reason?: string } {
  const name = basename(filePath).toLowerCase();

  // 业务派生临时缩略图，上传完成后可能被清理，不能入队
  if (/\.tg-thumb\.(jpg|jpeg|png|webp)$/i.test(name)) {
    return { skip: true, reason: 'derived_tg_thumb' };
  }

  // 常见临时/未完成下载文件，避免误入队导致 missing
  if (/\.(tmp|temp|part|crdownload)$/i.test(name)) {
    return { skip: true, reason: 'temporary_or_partial_file' };
  }

  return { skip: false };
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
    /\[第\s*(\d+)\s*(?:集|话|話)\]/,
    /第\s*(\d+)\s*(?:集|话|話)/,
    /S\d+E(\d+)/i,
  ];

  let episodeNo: number | null = null;
  for (const pattern of patterns) {
    const match = fileName.match(pattern);
    if (!match || !match[1]) continue;
    const parsed = Number(match[1]);
    if (!Number.isFinite(parsed) || parsed <= 0) continue;
    episodeNo = parsed;
    break;
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
  let groupedDiscovered = 0;
  const groupedKeys = new Set<string>();
  let textDirectTypeBCount = 0;

  for (const channel of channels) {
    let files: string[] = [];
    try {
      files = await scanChannelVideos(channel.folderPath);
    } catch {
      continue;
    }

    for (const filePath of files) {
      scannedFiles += 1;

      const skipDecision = shouldSkipRelayScanFile(filePath);
      if (skipDecision.skip) {
        logger.info('[relay] 扫描过滤派生/临时文件，跳过入队', {
          filePath,
          reason: skipDecision.reason ?? 'scan_filter',
        });
        continue;
      }

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
        const { pathNormalized, pathFingerprint } = buildRelayPathFingerprint(channel.id, filePath);

        const existedByPathFingerprint = await prisma.mediaAsset.findFirst({
          where: {
            channelId: channel.id,
            pathFingerprint,
          },
          select: {
            id: true,
            status: true,
            sourceMeta: true,
            telegramFileId: true,
            relayMessageId: true,
          },
        });

        if (existedByPathFingerprint) {
          logger.info('[中转] 路径指纹命中去重，复用已有资产', {
            频道ID: channel.id.toString(),
            文件路径: filePath,
            归一化路径: pathNormalized,
            路径指纹: pathFingerprint,
            资产ID: existedByPathFingerprint.id.toString(),
            处理动作: '命中去重',
            结果: '复用已有资产',
          });
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
              pathNormalized,
              pathFingerprint,
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
      const groupedMeta = parseGroupedMeta(filePath);
      const cloneSourceMeta = parseCloneSourceMeta(filePath, groupedMeta);
      const storedFileHash = buildStoredFileHash(fileHash, cloneSourceMeta);
      const collectionMeta = parseCollectionMeta(filePath);
      if (groupedMeta?.groupKey) {
        groupedDiscovered += 1;
        groupedKeys.add(groupedMeta.groupKey);
      }
      const sourceExpectedCount = await resolveSourceExpectedCount({
        channelId: channel.id,
        filePath,
        groupedMeta,
        cloneSourceMeta,
      });

      const ext = filePath.split('.').pop()?.toLowerCase() ?? '';
      const isTextOnlyCandidate = ext === 'txt' && !!groupedMeta;

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
                ...(groupedMeta ?? {}),
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

      if (isTextOnlyCandidate) {
        logger.info('[typea_group] text-only group candidate detected, route to typeb directly', {
          channelId: channel.id.toString(),
          filePath,
          groupKey: groupedMeta?.groupKey ?? null,
          groupedId: groupedMeta?.groupedId ?? null,
          routeMode: 'text_direct_typeb',
        });

        textDirectTypeBCount += 1;
        continue;
      }

      let asset = await prisma.mediaAsset.findFirst({
        where: {
          channelId: channel.id,
          pathFingerprint,
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
        asset = await prisma.mediaAsset.findUnique({
          where: {
            fileHash_fileSize: {
              fileHash: storedFileHash,
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
      }

      if (!asset) {
        try {
          asset = await prisma.mediaAsset.create({
            data: {
              channelId: channel.id,
              originalName: basename(filePath),
              localPath: filePath,
              pathNormalized,
              pathFingerprint,
              fileSize: BigInt(s.size),
              fileHash: storedFileHash,
              status: MediaStatus.ready,
              sourceMeta: {
                relayChannelId: definition.relayChannelId.toString(),
                taskDefinitionId: definition.id.toString(),
                relayEnqueueAt: new Date().toISOString(),
                relayPriority: definition.priority,
                relayMaxRetries: definition.maxRetries,
                ingestRetryCount: 0,
                ...(collectionMeta ?? {}),
                ...(groupedMeta ?? {}),
                ...(sourceExpectedCount && sourceExpectedCount > 0
                  ? { sourceExpectedCount }
                  : {}),
                ...(cloneSourceMeta ?? {}),
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
          logger.info('[中转] 路径指纹未命中，已创建资产', {
            频道ID: channel.id.toString(),
            文件路径: filePath,
            归一化路径: pathNormalized,
            路径指纹: pathFingerprint,
            资产ID: asset.id.toString(),
            处理动作: '新建资产',
            结果: '成功',
          });

          logger.info('[typeb_group][verify] sourceExpectedCount write snapshot', {
            channelId: channel.id.toString(),
            mediaAssetId: asset.id.toString(),
            filePath,
            groupKey: groupedMeta?.groupKey ?? null,
            groupedId: groupedMeta?.groupedId ?? null,
            cloneSourceMessageId: cloneSourceMeta?.sourceMessageId ?? null,
            cloneSourceChannelUsername: cloneSourceMeta?.sourceChannelUsername ?? null,
            sourceExpectedCount: sourceExpectedCount ?? null,
            sourceExpectedCountWriteApplied: Boolean(groupedMeta?.groupKey && sourceExpectedCount && sourceExpectedCount > 0),
            sourceExpectedCountWriteSource: groupedMeta?.groupKey
              ? sourceExpectedCount && sourceExpectedCount > 0
                ? 'asset_create_source_expected_count_from_clone_terminal_group_count'
                : 'asset_create_source_expected_count_missing'
              : 'none',
          });
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          if (!message.toLowerCase().includes('unique')) {
            throw error;
          }

          logger.warn('[中转] 路径指纹唯一冲突已自动恢复', {
            频道ID: channel.id.toString(),
            文件路径: filePath,
            归一化路径: pathNormalized,
            路径指纹: pathFingerprint,
            处理动作: '冲突恢复',
            结果: '回退查询',
            原因: message,
          });

          asset = await prisma.mediaAsset.findFirst({
            where: {
              channelId: channel.id,
              pathFingerprint,
            },
            select: {
              id: true,
              status: true,
              sourceMeta: true,
              telegramFileId: true,
              relayMessageId: true,
            },
          });
        }
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

      const cloneUploadedBySource = cloneSourceMeta
        ? await prisma.mediaAsset.findFirst({
            where: {
              channelId: channel.id,
              OR: [
                { status: MediaStatus.relay_uploaded },
                { telegramFileId: { not: null } },
                { relayMessageId: { not: null } },
              ],
              sourceMeta: {
                path: ['sourceMessageId'],
                equals: cloneSourceMeta.sourceMessageId,
              },
            },
            select: { id: true },
          })
        : null;

      const isAlreadyUploadedDuplicate = cloneSourceMeta
        ? Boolean(cloneUploadedBySource)
        : asset.status === MediaStatus.relay_uploaded ||
          Boolean(asset.telegramFileId) ||
          Boolean(asset.relayMessageId);

      if (isAlreadyUploadedDuplicate) {
        const duplicateName = basename(filePath);
        const duplicateError = `DUPLICATE_ALREADY_UPLOADED: 本地重复文件已跳过 ${duplicateName}`;
        const duplicateOfMediaAssetId = cloneUploadedBySource?.id?.toString?.() ?? asset.id.toString();

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
                duplicateOfMediaAssetId,
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
                duplicateOfMediaAssetId,
              },
            },
          });
        }

        await removeLocalDuplicateFile(filePath, duplicateOfMediaAssetId);
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
            ...(groupedMeta ?? {}),
            ...(sourceExpectedCount && sourceExpectedCount > 0
              ? { sourceExpectedCount }
              : {}),
            ...(cloneSourceMeta ?? {}),
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
    textDirectTypeBCount,
    groupedDiscovered,
    groupedDistinct: groupedKeys.size,
    relayChannelId: definition.relayChannelId.toString(),
  };
}

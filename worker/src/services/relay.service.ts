import { stat } from 'node:fs/promises';
import { basename } from 'node:path';
import { MediaStatus } from '@prisma/client';
import { prisma, getTaskDefinitionModel } from '../infra/prisma';
import { hashFile, scanChannelVideos, waitForFileStable } from '../shared/file-utils';
import { logger } from '../logger';

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

  let scannedFiles = 0;
  let createdAssets = 0;
  let enqueuedTasks = 0;

  for (const channel of channels) {
    let files: string[] = [];
    try {
      files = await scanChannelVideos(channel.folderPath);
    } catch {
      continue;
    }

    for (const filePath of files) {
      scannedFiles += 1;

      try {
        await waitForFileStable(filePath);
      } catch (error) {
        logger.warn('[relay] 文件未稳定或不可用，跳过', {
          filePath,
          reason: error instanceof Error ? error.message : String(error),
        });
        continue;
      }

      const s = await stat(filePath);
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
        select: { id: true, status: true },
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
          select: { id: true, status: true },
        });
        createdAssets += 1;
      }

      if (
        asset.status === MediaStatus.relay_uploaded ||
        asset.status === MediaStatus.ingesting ||
        asset.status === MediaStatus.failed
      ) {
        continue;
      }

      await prisma.mediaAsset.update({
        where: { id: asset.id },
        data: {
          status: MediaStatus.ready,
          sourceMeta: {
            relayChannelId: definition.relayChannelId.toString(),
            taskDefinitionId: definition.id.toString(),
            relayEnqueueAt: new Date().toISOString(),
            relayPriority: definition.priority,
            relayMaxRetries: definition.maxRetries,
          },
        },
      });

      enqueuedTasks += 1;
    }
  }

  return {
    scannedFiles,
    createdAssets,
    enqueuedTasks,
    relayChannelId: definition.relayChannelId.toString(),
  };
}

import { Worker } from 'bullmq';
import { openAsBlob } from 'node:fs';
import { basename } from 'node:path';
import { connection } from '../infra/redis';
import { prisma } from '../infra/prisma';
import { logger, logError } from '../logger';
import { moveToArchive, waitForFileStable } from '../shared/file-utils';
import { sendTelegramRequest } from '../shared/telegram';
import { MediaStatus } from '@prisma/client';

export const relayUploadWorker = new Worker(
  'q_relay_upload',
  async (job) => {
    if (job.name === 'bootstrap-check') {
      return { ok: true, skipped: true, reason: 'bootstrap-check' };
    }

    const mediaAssetIdRaw = job.data.mediaAssetId as string | undefined;
    const relayChannelIdRaw = job.data.relayChannelId as string | undefined;

    if (!mediaAssetIdRaw || !relayChannelIdRaw) {
      throw new Error('Missing mediaAssetId or relayChannelId in relay upload job payload');
    }

    const mediaAssetId = BigInt(mediaAssetIdRaw);

    const mediaAsset = await prisma.mediaAsset.findUnique({
      where: { id: mediaAssetId },
      include: {
        channel: {
          select: {
            id: true,
            name: true,
          },
        },
      },
    });

    if (!mediaAsset) {
      throw new Error(`Media asset not found: ${mediaAssetIdRaw}`);
    }

    const relayChannel = await prisma.relayChannel.findUnique({
      where: { id: BigInt(relayChannelIdRaw) },
      include: {
        bot: {
          select: {
            id: true,
            status: true,
            tokenEncrypted: true,
          },
        },
      },
    });

    if (!relayChannel) {
      throw new Error(`Relay channel not found: ${relayChannelIdRaw}`);
    }

    if (!relayChannel.isActive) {
      throw new Error(`Relay channel is inactive: ${relayChannelIdRaw}`);
    }

    if (!relayChannel.bot || relayChannel.bot.status !== 'active') {
      throw new Error('Relay channel bot is missing or inactive');
    }

    await waitForFileStable(mediaAsset.localPath);

    const formData = new FormData();
    formData.append('chat_id', relayChannel.tgChatId.toString());
    formData.append('caption', mediaAsset.originalName);
    formData.append('video', await openAsBlob(mediaAsset.localPath), basename(mediaAsset.localPath));
    formData.append('supports_streaming', 'true');

    logger.info('[q_relay_upload] uploading media asset', {
      mediaAssetId: mediaAssetIdRaw,
      relayChannelId: relayChannelIdRaw,
    });

    const sendResult = await sendTelegramRequest({
      botToken: relayChannel.bot.tokenEncrypted,
      method: 'sendVideo',
      payload: formData,
    });

    if (!sendResult.messageId || !sendResult.videoFileId) {
      throw new Error('Relay upload succeeded but missing telegram video file_id');
    }

    let archivePath: string | null = null;
    try {
      archivePath = await moveToArchive(mediaAsset.localPath);
    } catch (moveErr) {
      logError('[q_relay_upload] failed to move file to archive', moveErr);
    }

    await prisma.mediaAsset.update({
      where: { id: mediaAssetId },
      data: {
        status: MediaStatus.relay_uploaded,
        relayMessageId: BigInt(sendResult.messageId),
        telegramFileId: sendResult.videoFileId,
        telegramFileUniqueId: sendResult.videoFileUniqueId ?? null,
        ingestError: null,
        ...(archivePath ? { archivePath, localPath: archivePath } : {}),
      },
    });

    return {
      ok: true,
      mediaAssetId: mediaAssetIdRaw,
      relayChannelId: relayChannelIdRaw,
      messageId: sendResult.messageId,
    };
  },
  { connection: connection as any, concurrency: 2 },
);

relayUploadWorker.on('completed', (job) => {
  logger.info('[q_relay_upload] completed job', { jobId: String(job.id) });
});

relayUploadWorker.on('failed', async (job, err) => {
  const mediaAssetIdRaw = job?.data?.mediaAssetId as string | undefined;
  if (mediaAssetIdRaw) {
    await prisma.mediaAsset.update({
      where: { id: BigInt(mediaAssetIdRaw) },
      data: {
        status: MediaStatus.failed,
        ingestError: err.message,
      },
    });
  }

  logError('[q_relay_upload] failed job', {
    jobId: job?.id ? String(job.id) : null,
    mediaAssetId: mediaAssetIdRaw ?? null,
    error: err,
  });
});

relayUploadWorker.on('error', (err) => {
  logError('[q_relay_upload] worker error', err);
});

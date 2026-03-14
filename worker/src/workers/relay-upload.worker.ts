import { Worker } from 'bullmq';
import { openAsBlob } from 'node:fs';
import { basename } from 'node:path';
import { connection, backfillQueue } from '../infra/redis';
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
      throw new Error('中转上传任务缺少 mediaAssetId 或 relayChannelId');
    }

    const mediaAssetId = BigInt(mediaAssetIdRaw);
    const traceId = `relay-upload-${mediaAssetIdRaw}-${Date.now()}`;

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
      throw new Error(`未找到媒体资源: ${mediaAssetIdRaw}`);
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
      throw new Error(`未找到中转频道: ${relayChannelIdRaw}`);
    }

    if (!relayChannel.isActive) {
      throw new Error(`中转频道未启用: ${relayChannelIdRaw}`);
    }

    if (!relayChannel.bot || relayChannel.bot.status !== 'active') {
      throw new Error('中转频道机器人不存在或未启用');
    }

    const stableCheckStart = Date.now();
    await waitForFileStable(mediaAsset.localPath);
    const stableCheckDurationMs = Date.now() - stableCheckStart;
    if (stableCheckDurationMs > 1000) {
      logger.info('[q_relay_upload] 文件稳定性检查耗时', {
        traceId,
        stage: 'wait_for_stable',
        mediaAssetId: mediaAssetIdRaw,
        durationMs: stableCheckDurationMs,
      });
    }

    const formData = new FormData();
    formData.append('chat_id', relayChannel.tgChatId.toString());
    formData.append('caption', mediaAsset.originalName);

    const useDocument = mediaAsset.fileSize && mediaAsset.fileSize >= BigInt(900 * 1024 * 1024);
    const blobStart = Date.now();
    if (useDocument) {
      formData.append('document', await openAsBlob(mediaAsset.localPath), basename(mediaAsset.localPath));
    } else {
      formData.append('video', await openAsBlob(mediaAsset.localPath), basename(mediaAsset.localPath));
      formData.append('supports_streaming', 'true');
    }
    const blobDurationMs = Date.now() - blobStart;
    if (blobDurationMs > 500) {
      logger.info('[q_relay_upload] 文件读取/封装耗时', {
        traceId,
        stage: 'blob_build',
        mediaAssetId: mediaAssetIdRaw,
        durationMs: blobDurationMs,
      });
    }

    logger.info('[q_relay_upload] 开始上传媒体资源', {
      traceId,
      stage: 'start',
      mediaAssetId: mediaAssetIdRaw,
      relayChannelId: relayChannelIdRaw,
      uploadMethod: useDocument ? 'sendDocument' : 'sendVideo',
    });

    const uploadStart = Date.now();

    let sendResult;
    try {
      sendResult = await sendTelegramRequest({
      botToken: relayChannel.bot.tokenEncrypted,
      method: useDocument ? 'sendDocument' : 'sendVideo',
      payload: formData,
    });
    } catch (err: any) {
      logger.info('[q_relay_upload] 上传请求耗时', {
        traceId,
        stage: 'send_request',
        mediaAssetId: mediaAssetIdRaw,
        relayChannelId: relayChannelIdRaw,
        durationMs: Date.now() - uploadStart,
      });

      const errorCode = err?.code ?? null;
      if (errorCode === 'TG_SOCKET_HANG_UP') {
        await prisma.mediaAsset.update({
          where: { id: mediaAssetId },
          data: {
            status: MediaStatus.ingesting,
            ingestError: null,
          },
        });

        await backfillQueue.add(
          'check-file-id',
          {
            mediaAssetId: mediaAssetId.toString(),
            chatId: relayChannel.tgChatId.toString(),
            fileName: mediaAsset.originalName,
          },
          {
            delay: 180000 + Math.floor(Math.random() * 30000),
            attempts: 10,
            backoff: { type: 'exponential', delay: 60000 },
            removeOnComplete: true,
            removeOnFail: 200,
          },
        );

        logger.warn('[q_relay_upload] 上传连接中断，转入回查队列', {
          traceId,
          mediaAssetId: mediaAssetIdRaw,
          relayChannelId: relayChannelIdRaw,
          errorCode,
        });

        return {
          ok: true,
          pending: true,
          mediaAssetId: mediaAssetIdRaw,
          relayChannelId: relayChannelIdRaw,
          viaBackfill: true,
        };
      }

      throw err;
    }

    if (!sendResult.messageId) {
      throw new Error('中转上传成功但缺少 Telegram message_id');
    }

    logger.info('[q_relay_upload] 上传请求耗时', {
      traceId,
      stage: 'send_request',
      mediaAssetId: mediaAssetIdRaw,
      relayChannelId: relayChannelIdRaw,
      durationMs: Date.now() - uploadStart,
    });

    const archiveStart = Date.now();
    let archivePath: string | null = null;
    try {
      archivePath = await moveToArchive(mediaAsset.localPath);
    } catch (moveErr) {
      logError('[q_relay_upload] 归档文件失败', moveErr);
    }
    const archiveDurationMs = Date.now() - archiveStart;
    if (archiveDurationMs > 500) {
      logger.info('[q_relay_upload] 归档耗时', {
        traceId,
        stage: 'archive',
        mediaAssetId: mediaAssetIdRaw,
        durationMs: archiveDurationMs,
      });
    }

    const directFileId = sendResult.videoFileId ?? sendResult.documentFileId ?? sendResult.animationFileId;
    const directFileUniqueId = sendResult.videoFileUniqueId ?? sendResult.documentFileUniqueId ?? sendResult.animationFileUniqueId ?? null;

    if (directFileId) {
      // 🟢 小文件直接出了 file_id，直接标记成功
      await prisma.mediaAsset.update({
        where: { id: mediaAssetId },
        data: {
          status: MediaStatus.relay_uploaded,
          relayMessageId: BigInt(sendResult.messageId),
          telegramFileId: directFileId,
          telegramFileUniqueId: directFileUniqueId,
          ingestError: null,
          ...(archivePath ? { archivePath, localPath: archivePath } : {}),
        },
      });

      return {
        ok: true,
        direct: true,
        mediaAssetId: mediaAssetIdRaw,
        relayChannelId: relayChannelIdRaw,
        messageId: sendResult.messageId,
      };
    } else {
      // 🟡 大文件暂无 file_id，保持 ingesting 状态，记下凭证，推入回查队列
      await prisma.mediaAsset.update({
        where: { id: mediaAssetId },
        data: {
          status: MediaStatus.ingesting,
          relayMessageId: BigInt(sendResult.messageId),
          ingestError: null,
          ...(archivePath ? { archivePath, localPath: archivePath } : {}),
        },
      });

      await backfillQueue.add(
        'check-file-id',
        {
          mediaAssetId: mediaAssetId.toString(),
          chatId: relayChannel.tgChatId.toString(),
          messageId: sendResult.messageId.toString(),
        },
        {
          delay: 120000 + Math.floor(Math.random() * 30000), // 2分钟延迟 + 随机抖动防拥挤
          attempts: 10,
          backoff: { type: 'exponential', delay: 60000 },
          removeOnComplete: true,
          removeOnFail: 200,
        },
      );

      logger.info('[q_relay_upload] 大文件已发送，进入回查队列等待 file_id', { messageId: sendResult.messageId });

      return {
        ok: true,
        pending: true,
        mediaAssetId: mediaAssetIdRaw,
        relayChannelId: relayChannelIdRaw,
        messageId: sendResult.messageId,
      };
    }
  },
  {
    connection: connection as any,
    concurrency: 1,
    lockDuration: 30 * 60 * 1000,
    maxStalledCount: 1,
    stalledInterval: 30000,
  },
);

relayUploadWorker.on('completed', (job) => {
  logger.info('[q_relay_upload] 任务完成', { jobId: String(job.id) });
});

relayUploadWorker.on('failed', async (job, err) => {
  const mediaAssetIdRaw = job?.data?.mediaAssetId as string | undefined;
  if (mediaAssetIdRaw) {
    await prisma.mediaAsset.update({
      where: { id: BigInt(mediaAssetIdRaw) },
      data: {
        status: MediaStatus.failed,
        ingestError: err instanceof Error ? err.message : '未知错误',
      },
    });
  }

  let errorJson: string | null = null;
  if (!(err instanceof Error)) {
    try {
      errorJson = JSON.stringify(err);
    } catch {
      errorJson = null;
    }
  }

  const errorInfo = err instanceof Error
    ? { name: err.name, message: err.message, stack: err.stack }
    : { raw: err, string: String(err), json: errorJson };

  logError('[q_relay_upload] 任务失败', {
    jobId: job?.id ? String(job.id) : null,
    mediaAssetId: mediaAssetIdRaw ?? null,
    error: errorInfo,
  });
});

relayUploadWorker.on('error', (err) => {
  logError('[q_relay_upload] Worker 异常', err);
});
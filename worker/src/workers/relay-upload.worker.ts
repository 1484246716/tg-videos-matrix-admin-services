import { Worker } from 'bullmq';
import { createReadStream } from 'node:fs';
import { basename } from 'node:path';
import { PassThrough } from 'node:stream';
import FormData from 'form-data';
import { connection } from '../infra/redis';
import { prisma } from '../infra/prisma';
import { logger, logError } from '../logger';
import { moveToArchive, waitForFileStable } from '../shared/file-utils';
import { sendViaGramjs } from '../shared/gramjs/upload';
import { getTelegramUpdates, sendTelegramRequest } from '../shared/telegram';
import { MediaStatus } from '@prisma/client';
import {
  GRAMJS_FORWARD_TARGET_CHAT_ID,
  GRAMJS_UPLOAD_WORKERS,
  RELAY_UPLOAD_GRAMJS_THRESHOLD_MB,
} from '../config/env';

const PROGRESS_TTL_SECONDS = 24 * 60 * 60;
const PROGRESS_WRITE_INTERVAL_MS = 5000;

function buildProgressKey(mediaAssetId: string) {
  return `media:progress:${mediaAssetId}`;
}

async function writeProgress(params: {
  mediaAssetId: string;
  streamedBytes: number;
  totalBytes?: number;
  progress: number;
}) {
  const payload = {
    mediaAssetId: params.mediaAssetId,
    streamedBytes: params.streamedBytes,
    totalBytes: params.totalBytes ?? null,
    progress: Number(params.progress.toFixed(2)),
    updatedAt: new Date().toISOString(),
  };

  await connection.set(
    buildProgressKey(params.mediaAssetId),
    JSON.stringify(payload),
    'EX',
    PROGRESS_TTL_SECONDS,
  );
}

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

    if (mediaAsset.status === MediaStatus.relay_uploaded) {
      logger.info('[q_relay_upload] 任务已完成，跳过重复执行', {
        traceId,
        stage: 'skip_already_uploaded',
        mediaAssetId: mediaAssetIdRaw,
      });
      return {
        ok: true,
        skipped: true,
        reason: 'already_uploaded',
        mediaAssetId: mediaAssetIdRaw,
      };
    }

    if (!mediaAsset.channelId) {
      throw new Error(`媒体资源缺少关联频道: ${mediaAssetIdRaw}`);
    }

    const channel = await prisma.channel.findUnique({
      where: { id: mediaAsset.channelId },
      select: { id: true, defaultBotId: true, status: true },
    });

    if (!channel) {
      throw new Error(`未找到媒体所属频道: ${mediaAsset.channelId.toString()}`);
    }

    if (!channel.defaultBotId) {
      throw new Error(`频道未绑定机器人: channelId=${channel.id.toString()}`);
    }

    const relayChannel = await prisma.relayChannel.findFirst({
      where: {
        botId: channel.defaultBotId,
        isActive: true,
      },
      include: {
        bot: {
          select: { id: true, tokenEncrypted: true, status: true },
        },
      },
      orderBy: [{ id: 'asc' }],
    });

    if (!relayChannel) {
      throw new Error(
        `未找到可用中转频道: channelId=${channel.id.toString()}, botId=${channel.defaultBotId.toString()}`,
      );
    }

    if (relayChannel.bot.status !== 'active') {
      throw new Error(
        `中转频道绑定机器人未启用: relayChannelId=${relayChannel.id.toString()}, botId=${relayChannel.bot.id.toString()}`,
      );
    }

    const selectedRelayChannelId = relayChannel.id?.toString?.() ?? String(relayChannel.id);

    logger.info('[q_relay_upload] 固定选中中转频道(按频道绑定bot)', {
      traceId,
      mediaAssetId: mediaAssetIdRaw,
      channelId: channel.id.toString(),
      channelBotId: channel.defaultBotId.toString(),
      relayChannelId: selectedRelayChannelId,
      relayBotId: relayChannel.bot.id.toString(),
      sourceRelayChannelId: relayChannelIdRaw,
      pickMode: 'fixed_by_channel_bot',
    });

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

    const fileName = basename(mediaAsset.localPath);
    const fileSize = mediaAsset.fileSize ? Number(mediaAsset.fileSize) : undefined;
    const gramjsThresholdBytes = RELAY_UPLOAD_GRAMJS_THRESHOLD_MB * 1024 * 1024;
    const useGramjs = fileSize !== undefined && fileSize >= gramjsThresholdBytes;

    if (useGramjs) {
      logger.info('[q_relay_upload] 使用 GramJS 上传大文件', {
        traceId,
        stage: 'start',
        mediaAssetId: mediaAssetIdRaw,
        relayChannelId: relayChannelIdRaw,
        uploadMethod: 'gramjs',
        fileSize,
      });

      const uploadStart = Date.now();
      let lastProgressWriteAt = 0;

      let gramjsMessageId: number;
      try {
        const gramjsResult = await sendViaGramjs({
          filePath: mediaAsset.localPath,
          fileName,
          caption: mediaAsset.originalName,
          chatId: relayChannel.tgChatId.toString(),
          workers: GRAMJS_UPLOAD_WORKERS,
          progressCallback: (progress) => {
            const percent = Number((progress * 100).toFixed(2));
            logger.info('[q_relay_upload] GramJS 上传进度', {
              traceId,
              stage: 'gramjs_progress',
              mediaAssetId: mediaAssetIdRaw,
              relayChannelId: relayChannelIdRaw,
              progress: percent,
            });

            const now = Date.now();
            if (now - lastProgressWriteAt >= PROGRESS_WRITE_INTERVAL_MS) {
              void writeProgress({
                mediaAssetId: mediaAssetIdRaw,
                streamedBytes: fileSize ? Math.floor((fileSize * percent) / 100) : 0,
                totalBytes: fileSize,
                progress: percent,
              });
              lastProgressWriteAt = now;
            }
          },
        });

        gramjsMessageId = gramjsResult.messageId;
      } catch (err) {
        logError('[q_relay_upload] GramJS 上传失败', err);
        throw err instanceof Error ? err : new Error('GramJS 上传失败');
      }

      logger.info('[q_relay_upload] 上传请求耗时', {
        traceId,
        stage: 'send_request',
        mediaAssetId: mediaAssetIdRaw,
        relayChannelId: relayChannelIdRaw,
        durationMs: Date.now() - uploadStart,
      });

      const forwardTargetChatId =
        GRAMJS_FORWARD_TARGET_CHAT_ID || relayChannel.tgChatId.toString();

      let forwardResult;
      try {
        forwardResult = await sendTelegramRequest({
          botToken: relayChannel.bot.tokenEncrypted,
          method: 'forwardMessage',
          payload: {
            chat_id: forwardTargetChatId,
            from_chat_id: relayChannel.tgChatId.toString(),
            message_id: gramjsMessageId,
            disable_notification: true,
          },
        });
      } catch (forwardErr) {
        logError('[q_relay_upload] forwardMessage 失败', forwardErr);
        throw forwardErr instanceof Error ? forwardErr : new Error('forwardMessage 失败');
      }

      const forwardFileId =
        forwardResult.videoFileId ??
        forwardResult.documentFileId ??
        forwardResult.animationFileId;
      const forwardFileUniqueId =
        forwardResult.videoFileUniqueId ??
        forwardResult.documentFileUniqueId ??
        forwardResult.animationFileUniqueId ??
        null;

      if (!forwardFileId) {
        await prisma.mediaAsset.update({
          where: { id: mediaAssetId },
          data: {
            status: MediaStatus.failed,
            relayMessageId: BigInt(gramjsMessageId),
            ingestError: 'GramJS 上传成功但 forwardMessage 未返回 file_id',
          },
        });

        throw new Error('GramJS 上传成功但 forwardMessage 未返回 file_id');
      }

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

      await prisma.mediaAsset.update({
        where: { id: mediaAssetId },
        data: {
          status: MediaStatus.relay_uploaded,
          relayMessageId: BigInt(gramjsMessageId),
          telegramFileId: forwardFileId,
          telegramFileUniqueId: forwardFileUniqueId,
          ingestError: null,
          sourceMeta: {
            ...(mediaAsset.sourceMeta && typeof mediaAsset.sourceMeta === 'object'
              ? (mediaAsset.sourceMeta as Record<string, unknown>)
              : {}),
            relayBotId: relayChannel.bot.id.toString(),
          },
          ...(archivePath ? { archivePath, localPath: archivePath } : {}),
        },
      });

      await writeProgress({
        mediaAssetId: mediaAssetIdRaw,
        streamedBytes: fileSize ?? 0,
        totalBytes: fileSize,
        progress: 100,
      });

      return {
        ok: true,
        direct: true,
        mediaAssetId: mediaAssetIdRaw,
        relayChannelId: relayChannelIdRaw,
        messageId: gramjsMessageId,
      };
    }

    await writeProgress({
      mediaAssetId: mediaAssetIdRaw,
      streamedBytes: 0,
      totalBytes: fileSize,
      progress: 0,
    });

    const formData = new FormData();
    formData.append('chat_id', relayChannel.tgChatId.toString());
    formData.append('caption', mediaAsset.originalName);

    const useDocument =
      mediaAsset.fileSize && mediaAsset.fileSize >= BigInt(900 * 1024 * 1024);
    const streamStart = Date.now();
    const fileStream = createReadStream(mediaAsset.localPath);
    const streamWithProgress = new PassThrough();
    let streamedBytes = 0;
    let lastProgressAt = Date.now();
    let lastProgressWriteAt = 0;

    fileStream.on('data', (chunk) => {
      streamedBytes += chunk.length;
      const now = Date.now();
      if (now - lastProgressAt >= 30 * 1000) {
        logger.info('[q_relay_upload] 上传字节进度', {
          traceId,
          stage: 'upload_progress_bytes',
          mediaAssetId: mediaAssetIdRaw,
          relayChannelId: relayChannelIdRaw,
          streamedBytes,
          fileSize,
        });
        lastProgressAt = now;
      }

      if (now - lastProgressWriteAt >= PROGRESS_WRITE_INTERVAL_MS) {
        const progress = fileSize ? (streamedBytes / fileSize) * 100 : 0;
        void writeProgress({
          mediaAssetId: mediaAssetIdRaw,
          streamedBytes,
          totalBytes: fileSize,
          progress,
        });
        lastProgressWriteAt = now;
      }
    });

    fileStream.on('error', (err) => {
      streamWithProgress.destroy(err);
    });

    fileStream.pipe(streamWithProgress);

    if (useDocument) {
      formData.append('document', streamWithProgress, {
        filename: fileName,
        knownLength: fileSize,
      });
    } else {
      formData.append('video', streamWithProgress, {
        filename: fileName,
        knownLength: fileSize,
      });
      formData.append('supports_streaming', 'true');
    }
    const streamDurationMs = Date.now() - streamStart;
    if (streamDurationMs > 500) {
      logger.info('[q_relay_upload] 文件读取/封装耗时', {
        traceId,
        stage: 'blob_build',
        mediaAssetId: mediaAssetIdRaw,
        durationMs: streamDurationMs,
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
    const uploadStartAt = new Date();

    const progressTicker = setInterval(() => {
      logger.info('[q_relay_upload] 上传进行中', {
        traceId,
        stage: 'upload_progress',
        mediaAssetId: mediaAssetIdRaw,
        relayChannelId: relayChannelIdRaw,
        elapsedMs: Date.now() - uploadStart,
      });
    }, 60 * 1000);

    let sendResult;
    try {
      sendResult = await sendTelegramRequest({
        botToken: relayChannel.bot.tokenEncrypted,
        method: useDocument ? 'sendDocument' : 'sendVideo',
        payload: formData,
      });
    } catch (err: any) {
      clearInterval(progressTicker);
      logger.info('[q_relay_upload] 上传请求耗时', {
        traceId,
        stage: 'send_request',
        mediaAssetId: mediaAssetIdRaw,
        relayChannelId: relayChannelIdRaw,
        durationMs: Date.now() - uploadStart,
      });

      const errorCode = err?.code ?? null;
      const errorMessage = err?.message ?? null;
      if (errorCode === 'TG_SOCKET_HANG_UP') {
        logger.warn('[q_relay_upload] 命中连接中断兜底条件', {
          traceId,
          mediaAssetId: mediaAssetIdRaw,
          relayChannelId: relayChannelIdRaw,
          errorCode,
          errorMessage,
        });

        const fallbackResult = await tryRecoverAfterHangUp({
          botToken: relayChannel.bot.tokenEncrypted,
          relayChannelId: relayChannel.tgChatId.toString(),
          originalName: mediaAsset.originalName,
          fileSize: fileSize ?? undefined,
          uploadStartAt,
        });

        if (fallbackResult) {
          await prisma.mediaAsset.update({
            where: { id: mediaAssetId },
            data: {
              status: MediaStatus.relay_uploaded,
              relayMessageId: BigInt(fallbackResult.messageId),
              telegramFileId: fallbackResult.telegramFileId,
              telegramFileUniqueId: fallbackResult.telegramFileUniqueId,
              ingestError: null,
              sourceMeta: {
                ...(mediaAsset.sourceMeta && typeof mediaAsset.sourceMeta === 'object'
                  ? (mediaAsset.sourceMeta as Record<string, unknown>)
                  : {}),
                relayBotId: relayChannel.bot.id.toString(),
              },
            },
          });

          logger.info('[q_relay_upload] 终极兜底补偿成功', {
            traceId,
            mediaAssetId: mediaAssetIdRaw,
            relayChannelId: relayChannelIdRaw,
            messageId: fallbackResult.messageId,
          });

          return {
            ok: true,
            direct: false,
            compensated: true,
            mediaAssetId: mediaAssetIdRaw,
            relayChannelId: relayChannelIdRaw,
            messageId: fallbackResult.messageId,
          };
        }

        throw new Error('上传连接中断');
      }

      throw err;
    }

    if (!sendResult.messageId) {
      clearInterval(progressTicker);
      throw new Error('中转上传成功但缺少 Telegram message_id');
    }

    clearInterval(progressTicker);

    await writeProgress({
      mediaAssetId: mediaAssetIdRaw,
      streamedBytes,
      totalBytes: fileSize,
      progress: fileSize ? (streamedBytes / fileSize) * 100 : 100,
    });

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

    const directFileId =
      sendResult.videoFileId ??
      sendResult.documentFileId ??
      sendResult.animationFileId;
    const directFileUniqueId =
      sendResult.videoFileUniqueId ??
      sendResult.documentFileUniqueId ??
      sendResult.animationFileUniqueId ??
      null;

    if (directFileId) {
      await prisma.mediaAsset.update({
        where: { id: mediaAssetId },
        data: {
          status: MediaStatus.relay_uploaded,
          relayMessageId: BigInt(sendResult.messageId),
          telegramFileId: directFileId,
          telegramFileUniqueId: directFileUniqueId,
          ingestError: null,
          sourceMeta: {
            ...(mediaAsset.sourceMeta && typeof mediaAsset.sourceMeta === 'object'
              ? (mediaAsset.sourceMeta as Record<string, unknown>)
              : {}),
            relayBotId: relayChannel.bot.id.toString(),
          },
          ...(archivePath ? { archivePath, localPath: archivePath } : {}),
        },
      });

      await writeProgress({
        mediaAssetId: mediaAssetIdRaw,
        streamedBytes: fileSize ?? streamedBytes,
        totalBytes: fileSize,
        progress: 100,
      });

      return {
        ok: true,
        direct: true,
        mediaAssetId: mediaAssetIdRaw,
        relayChannelId: relayChannelIdRaw,
        messageId: sendResult.messageId,
      };
    } else {
      await prisma.mediaAsset.update({
        where: { id: mediaAssetId },
        data: {
          status: MediaStatus.failed,
          relayMessageId: BigInt(sendResult.messageId),
          ingestError: '中转上传成功但未返回 file_id',
          ...(archivePath ? { archivePath, localPath: archivePath } : {}),
        },
      });

      throw new Error('中转上传成功但未返回 file_id');
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
  let shouldMarkFailed = true;

  if (mediaAssetIdRaw && err instanceof Error) {
    if (err.message.includes('ENOENT')) {
      const asset = await prisma.mediaAsset.findUnique({
        where: { id: BigInt(mediaAssetIdRaw) },
        select: { status: true },
      });
      if (asset?.status === MediaStatus.relay_uploaded) {
        shouldMarkFailed = false;
      }
    }
  }

  if (mediaAssetIdRaw && shouldMarkFailed) {
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

async function tryRecoverAfterHangUp(args: {
  botToken: string;
  relayChannelId: string;
  originalName: string;
  fileSize?: number;
  uploadStartAt: Date;
}) {
  await new Promise((resolve) => setTimeout(resolve, 4000));

  const { updates } = await getTelegramUpdates({
    botToken: args.botToken,
    limit: 50,
    timeoutSec: 3,
    allowedUpdates: ['channel_post'],
  });

  if (updates.length === 0) return null;

  const channelId = args.relayChannelId;
  const windowStart = args.uploadStartAt.getTime() - 10 * 60 * 1000;
  const windowEnd = Date.now() + 5 * 60 * 1000;

  const matched = updates.find((update) => {
    const post = update.channel_post;
    if (!post?.chat?.id) return false;
    if (String(post.chat.id) !== channelId) return false;

    const doc = post.document;
    const video = post.video;
    const fileName = doc?.file_name ?? video?.file_name;
    const fileSize = doc?.file_size ?? video?.file_size;
    const fileId = doc?.file_id ?? video?.file_id;

    if (!fileName || !fileSize || !fileId) return false;
    if (fileName !== args.originalName) return false;
    if (args.fileSize && fileSize !== args.fileSize) return false;

    const timestamp = post.date ? post.date * 1000 : 0;
    if (!timestamp || timestamp < windowStart || timestamp > windowEnd) return false;

    return true;
  });

  if (!matched?.channel_post) return null;

  const matchedDoc = matched.channel_post.document ?? matched.channel_post.video;
  if (!matchedDoc?.file_id || !matchedDoc?.file_unique_id) return null;

  return {
    messageId: matched.channel_post.message_id,
    telegramFileId: matchedDoc.file_id,
    telegramFileUniqueId: matchedDoc.file_unique_id,
  };
}

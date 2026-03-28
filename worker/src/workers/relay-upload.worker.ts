import { Worker } from 'bullmq';
import { createReadStream } from 'node:fs';
import { unlink } from 'node:fs/promises';
import { basename } from 'node:path';
import { PassThrough } from 'node:stream';
import FormData from 'form-data';
import { connection } from '../infra/redis';
import { prisma } from '../infra/prisma';
import { logger, logError } from '../logger';
import {
  createVideoThumbnail,
  ensureMp4Faststart,
  getVideoProbeMeta,
  waitForFileStable,
} from '../shared/file-utils';
import { sendViaGramjs } from '../shared/gramjs/upload';
import { getTelegramUpdates, sendTelegramRequest } from '../shared/telegram';
import { MediaStatus } from '@prisma/client';
import {
  GRAMJS_FORWARD_TARGET_CHAT_ID,
  GRAMJS_UPLOAD_WORKERS,
  RELAY_UPLOAD_GRAMJS_THRESHOLD_MB,
  RELAY_UPLOAD_QUEUE_CONCURRENCY,
  TYPEA_FAIL_ON_FILE_MISSING,
  TYPEA_INGEST_LEASE_MS,
  TYPEA_INGEST_MAX_RETRIES,
} from '../config/env';
import { TYPEA_INGEST_ERROR_CODE, TYPEA_INGEST_FINAL_REASON } from '../shared/metrics';

const PROGRESS_TTL_SECONDS = 24 * 60 * 60;
const PROGRESS_WRITE_INTERVAL_MS = 5000;

function calcIngestDurationSec(startedAt?: Date | null, finishedAt?: Date | null) {
  if (!startedAt || !finishedAt) return null;
  const diffMs = finishedAt.getTime() - startedAt.getTime();
  if (!Number.isFinite(diffMs) || diffMs < 0) return null;
  return Math.floor(diffMs / 1000);
}

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

async function removeUploadedSourceFile(filePath: string, context: {
  traceId: string;
  mediaAssetId: string;
  relayChannelId: string;
  uploadMethod: 'gramjs_sendVideo' | 'sendVideo' | 'recover_after_hang_up';
}) {
  try {
    await unlink(filePath);
    logger.info('[q_relay_upload] 上传成功后已删除源文件', {
      traceId: context.traceId,
      stage: 'delete_source_after_success',
      mediaAssetId: context.mediaAssetId,
      relayChannelId: context.relayChannelId,
      uploadMethod: context.uploadMethod,
      filePath,
    });
  } catch (error) {
    logger.warn('[q_relay_upload] 上传成功后删除源文件失败（不阻断成功状态）', {
      traceId: context.traceId,
      stage: 'delete_source_after_success_failed',
      mediaAssetId: context.mediaAssetId,
      relayChannelId: context.relayChannelId,
      uploadMethod: context.uploadMethod,
      filePath,
      reason: error instanceof Error ? error.message : String(error),
    });
  }
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

    const buildLeaseUntil = () =>
      new Date(Date.now() + TYPEA_INGEST_LEASE_MS).toISOString();

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

    const sourceMetaBase =
      mediaAsset.sourceMeta && typeof mediaAsset.sourceMeta === 'object'
        ? (mediaAsset.sourceMeta as Record<string, unknown>)
        : {};

    const ingestStartedAt = (mediaAsset as any).ingestStartedAt ?? new Date();
    if (!(mediaAsset as any).ingestStartedAt) {
      await prisma.mediaAsset.update({
        where: { id: mediaAssetId },
        data: {
          ingestStartedAt,
          ingestFinishedAt: null,
          ingestDurationSec: null,
        } as any,
      });
    }

    const heartbeat = async (extra?: Record<string, unknown>) => {
      await prisma.mediaAsset.update({
        where: { id: mediaAssetId },
        data: {
          sourceMeta: {
            ...sourceMetaBase,
            ...(extra || {}),
            ingestLeaseUntil: buildLeaseUntil(),
            ingestWorkerJobId: String(job.id),
            ingestLastHeartbeatAt: new Date().toISOString(),
          },
        },
      });
    };

    await heartbeat();

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

    let thumbnailPath: string | null = null;

    try {
      const stableCheckStart = Date.now();
      await heartbeat({ ingestStage: 'wait_for_stable' });
      await waitForFileStable(mediaAsset.localPath);

    try {
      await ensureMp4Faststart(mediaAsset.localPath);
    } catch (error) {
      logger.warn('[q_relay_upload] faststart 预处理跳过（不阻断上传）', {
        traceId,
        mediaAssetId: mediaAssetIdRaw,
        reason: error instanceof Error ? error.message : String(error),
      });
    }

    let videoMeta = {
      durationSec: null as number | null,
      width: null as number | null,
      height: null as number | null,
      supportsStreaming: true,
    };

    try {
      videoMeta = await getVideoProbeMeta(mediaAsset.localPath);
    } catch (error) {
      logger.warn('[q_relay_upload] ffprobe 视频元数据探测跳过（不阻断上传）', {
        traceId,
        mediaAssetId: mediaAssetIdRaw,
        reason: error instanceof Error ? error.message : String(error),
      });
    }

    const normalizedDurationSec =
      typeof videoMeta.durationSec === 'number' && Number.isFinite(videoMeta.durationSec) && videoMeta.durationSec > 0
        ? Math.floor(videoMeta.durationSec)
        : null;

    try {
      thumbnailPath = await createVideoThumbnail(mediaAsset.localPath);
    } catch (error) {
      logger.warn('[q_relay_upload] 视频封面生成失败，继续上传', {
        traceId,
        mediaAssetId: mediaAssetIdRaw,
        reason: error instanceof Error ? error.message : String(error),
      });
    }

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
        uploadMethod: 'gramjs_sendVideo',
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
          forceDocument: false,
          workers: GRAMJS_UPLOAD_WORKERS,
          videoMeta,
          thumbnailPath: thumbnailPath ?? undefined,
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

      const forwardFileId = forwardResult.videoFileId;
      const forwardFileUniqueId = forwardResult.videoFileUniqueId ?? null;

      if (!forwardFileId) {
        const ingestFinishedAt = new Date();
        await prisma.mediaAsset.update({
          where: { id: mediaAssetId },
          data: {
            status: MediaStatus.failed,
            relayMessageId: BigInt(gramjsMessageId),
            ingestError: 'GramJS 上传成功但 forwardMessage 未返回 video_file_id（疑似被识别为 document）',
            ingestFinishedAt,
            ingestDurationSec: calcIngestDurationSec(ingestStartedAt, ingestFinishedAt),
          } as any,
        });

        throw new Error('GramJS 上传成功但 forwardMessage 未返回 video_file_id');
      }

      const ingestFinishedAt = new Date();
      await prisma.mediaAsset.update({
        where: { id: mediaAssetId },
        data: {
          status: MediaStatus.relay_uploaded,
          relayMessageId: BigInt(gramjsMessageId),
          telegramFileId: forwardFileId,
          telegramFileUniqueId: forwardFileUniqueId,
          ingestError: null,
          durationSec: normalizedDurationSec,
          ingestFinishedAt,
          ingestDurationSec: calcIngestDurationSec(ingestStartedAt, ingestFinishedAt),
          archivePath: null,
          sourceMeta: {
            ...(mediaAsset.sourceMeta && typeof mediaAsset.sourceMeta === 'object'
              ? (mediaAsset.sourceMeta as Record<string, unknown>)
              : {}),
            relayBotId: relayChannel.bot.id.toString(),
            ingestLeaseUntil: null,
            ingestLastHeartbeatAt: new Date().toISOString(),
            ingestWorkerJobId: null,
            ingestStage: 'done',
          },
        } as any,
      });

      await removeUploadedSourceFile(mediaAsset.localPath, {
        traceId,
        mediaAssetId: mediaAssetIdRaw,
        relayChannelId: relayChannelIdRaw,
        uploadMethod: 'gramjs_sendVideo',
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

    formData.append('video', streamWithProgress, {
      filename: fileName,
      knownLength: fileSize,
    });
    if (thumbnailPath) {
      formData.append('thumbnail', createReadStream(thumbnailPath), {
        filename: 'thumb.jpg',
      });
    }
    formData.append('supports_streaming', 'true');
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
      uploadMethod: 'sendVideo',
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

    const heartbeatTicker = setInterval(() => {
      void heartbeat({ ingestStage: 'uploading' });
    }, 20 * 1000);

    let sendResult;
    try {
      sendResult = await sendTelegramRequest({
        botToken: relayChannel.bot.tokenEncrypted,
        method: 'sendVideo',
        payload: formData,
      });
    } catch (err: any) {
      clearInterval(progressTicker);
      clearInterval(heartbeatTicker);
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
          const ingestFinishedAt = new Date();
          await prisma.mediaAsset.update({
            where: { id: mediaAssetId },
            data: {
              status: MediaStatus.relay_uploaded,
              relayMessageId: BigInt(fallbackResult.messageId),
              telegramFileId: fallbackResult.telegramFileId,
              telegramFileUniqueId: fallbackResult.telegramFileUniqueId,
              ingestError: null,
              durationSec: normalizedDurationSec,
              ingestFinishedAt,
              ingestDurationSec: calcIngestDurationSec(ingestStartedAt, ingestFinishedAt),
              archivePath: null,
              sourceMeta: {
                ...(mediaAsset.sourceMeta && typeof mediaAsset.sourceMeta === 'object'
                  ? (mediaAsset.sourceMeta as Record<string, unknown>)
                  : {}),
                relayBotId: relayChannel.bot.id.toString(),
                ingestLeaseUntil: null,
                ingestLastHeartbeatAt: new Date().toISOString(),
                ingestWorkerJobId: null,
                ingestStage: 'done',
              },
            } as any,
          });

          await removeUploadedSourceFile(mediaAsset.localPath, {
            traceId,
            mediaAssetId: mediaAssetIdRaw,
            relayChannelId: relayChannelIdRaw,
            uploadMethod: 'recover_after_hang_up',
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
      clearInterval(heartbeatTicker);
      throw new Error('中转上传成功但缺少 Telegram message_id');
    }

    clearInterval(progressTicker);
    clearInterval(heartbeatTicker);

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

    const directFileId = sendResult.videoFileId;
    const directFileUniqueId = sendResult.videoFileUniqueId ?? null;

    if (directFileId) {
      const ingestFinishedAt = new Date();
      await prisma.mediaAsset.update({
        where: { id: mediaAssetId },
        data: {
          status: MediaStatus.relay_uploaded,
          relayMessageId: BigInt(sendResult.messageId),
          telegramFileId: directFileId,
          telegramFileUniqueId: directFileUniqueId,
          ingestError: null,
          durationSec: normalizedDurationSec,
          ingestFinishedAt,
          ingestDurationSec: calcIngestDurationSec(ingestStartedAt, ingestFinishedAt),
          archivePath: null,
          sourceMeta: {
            ...(mediaAsset.sourceMeta && typeof mediaAsset.sourceMeta === 'object'
              ? (mediaAsset.sourceMeta as Record<string, unknown>)
              : {}),
            relayBotId: relayChannel.bot.id.toString(),
            ingestLeaseUntil: null,
            ingestLastHeartbeatAt: new Date().toISOString(),
            ingestWorkerJobId: null,
            ingestStage: 'done',
          },
        } as any,
      });

      await removeUploadedSourceFile(mediaAsset.localPath, {
        traceId,
        mediaAssetId: mediaAssetIdRaw,
        relayChannelId: relayChannelIdRaw,
        uploadMethod: 'sendVideo',
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
      const ingestFinishedAt = new Date();
      await prisma.mediaAsset.update({
        where: { id: mediaAssetId },
        data: {
          status: MediaStatus.failed,
          relayMessageId: BigInt(sendResult.messageId),
          ingestError: '中转上传成功但未返回 video_file_id（疑似被识别为 document）',
          ingestFinishedAt,
          ingestDurationSec: calcIngestDurationSec(ingestStartedAt, ingestFinishedAt),
        } as any,
      });

      throw new Error('中转上传成功但未返回 video_file_id');
    }
  } finally {
    if (thumbnailPath) {
      try {
        await unlink(thumbnailPath);
      } catch {
        // ignore
      }
      thumbnailPath = null;
    }
  }
},
  {
    connection: connection as any,
    concurrency: RELAY_UPLOAD_QUEUE_CONCURRENCY,
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
    try {
      const asset = await prisma.mediaAsset.findUnique({
        where: { id: BigInt(mediaAssetIdRaw) },
        select: { status: true, sourceMeta: true },
      });

      const sourceMeta =
        asset?.sourceMeta && typeof asset.sourceMeta === 'object'
          ? (asset.sourceMeta as Record<string, unknown>)
          : {};

      const ingestRetryCountRaw = sourceMeta.ingestRetryCount;
      const ingestRetryCount =
        typeof ingestRetryCountRaw === 'number'
          ? ingestRetryCountRaw
          : typeof ingestRetryCountRaw === 'string' && /^\d+$/.test(ingestRetryCountRaw)
            ? Number(ingestRetryCountRaw)
            : 0;

      const message = err instanceof Error ? err.message : String(err);
      const isCommandMissing = /spawn\s+(ffmpeg|ffprobe)\s+ENOENT/i.test(message);
      const isFileMissing = message.includes('ENOENT: no such file or directory') && !isCommandMissing;
      // Telegram RPC 确定性错误（参数非法），重试无效，直接标记 failedFinal
      const isTelegramRpcFatal = /DOUBLE_VALUE_INVALID|INTEGER_VALUE_INVALID|PHOTO_INVALID_DIMENSIONS|FILE_PARTS_INVALID|MSG_ID_INVALID/i.test(message);
      const exceeded = ingestRetryCount >= TYPEA_INGEST_MAX_RETRIES;
      const finalOnMissing = TYPEA_FAIL_ON_FILE_MISSING && isFileMissing;
      const isFinal = exceeded || finalOnMissing || isTelegramRpcFatal;

      if (!asset) {
        logger.warn('[q_relay_upload] 任务失败回写跳过：mediaAsset 不存在', {
          mediaAssetId: mediaAssetIdRaw,
        });
      } else if (asset.status !== MediaStatus.relay_uploaded) {
        const ingestFinishedAt = new Date();
        await prisma.mediaAsset.updateMany({
          where: { id: BigInt(mediaAssetIdRaw) },
          data: {
            status: MediaStatus.failed,
            ingestError: isFileMissing
              ? 'SRC_FILE_MISSING: source file not found'
              : isCommandMissing
                ? 'TOOL_MISSING: ffmpeg/ffprobe not found in PATH'
                : isTelegramRpcFatal
                  ? `TG_RPC_FATAL: ${message}`
                  : message || '未知错误',
            ingestFinishedAt,
            ingestDurationSec: null,
            sourceMeta: {
              ...sourceMeta,
              ingestRetryCount,
              ingestErrorCode: isFileMissing
                ? TYPEA_INGEST_ERROR_CODE.srcFileMissing
                : TYPEA_INGEST_ERROR_CODE.ingestRuntimeError,
              ingestFinalReason: isFinal
                ? TYPEA_INGEST_FINAL_REASON.failedFinal
                : TYPEA_INGEST_FINAL_REASON.retryable,
              ingestLeaseUntil: null,
              ingestWorkerJobId: null,
              ingestLastHeartbeatAt: new Date().toISOString(),
              ingestStage: 'failed',
            },
          } as any,
        });
      }

      logger.info('[typea_metrics] relay upload failed', {
        mediaAssetId: mediaAssetIdRaw,
        typea_file_missing_total: isFileMissing ? 1 : 0,
        typea_failed_final_total: isFinal ? 1 : 0,
        task_run_total: 1,
        task_failed_total: 1,
        task_dead_total: isFinal ? 1 : 0,
        ingestRetryCount,
        maxRetries: TYPEA_INGEST_MAX_RETRIES,
        failOnFileMissing: TYPEA_FAIL_ON_FILE_MISSING,
        metric_labels: {
          typea_file_missing_total: 'TypeA 源文件缺失总数',
          typea_failed_final_total: 'TypeA 失败终态总数',
        },
      });
    } catch (writeBackErr) {
      logError('[q_relay_upload] 任务失败状态回写异常', {
        mediaAssetId: mediaAssetIdRaw,
        error: writeBackErr,
      });
    }
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

    const video = post.video;
    const fileName = video?.file_name;
    const fileSize = video?.file_size;
    const fileId = video?.file_id;

    if (!fileName || !fileSize || !fileId) return false;
    if (fileName !== args.originalName) return false;
    if (args.fileSize && fileSize !== args.fileSize) return false;

    const timestamp = post.date ? post.date * 1000 : 0;
    if (!timestamp || timestamp < windowStart || timestamp > windowEnd) return false;

    return true;
  });

  if (!matched?.channel_post?.video) return null;

  const matchedVideo = matched.channel_post.video;
  if (!matchedVideo?.file_id || !matchedVideo?.file_unique_id) return null;

  return {
    messageId: matched.channel_post.message_id,
    telegramFileId: matchedVideo.file_id,
    telegramFileUniqueId: matchedVideo.file_unique_id,
  };
}

import { Worker } from 'bullmq';
import { createReadStream } from 'node:fs';
import { readFile, unlink } from 'node:fs/promises';
import { basename, dirname, extname, join } from 'node:path';
import { PassThrough } from 'node:stream';
import FormData from 'form-data';
import { connection, relayUploadQueue } from '../infra/redis';
import { prisma } from '../infra/prisma';
import { logger, logError, toReadableErrorSummary } from '../logger';
import {
  createVideoThumbnail,
  ensureMp4Faststart,
  getVideoProbeMeta,
  waitForFileStable,
} from '../shared/file-utils';
import { sendViaGramjs } from '../shared/gramjs/upload';
import { getTelegramUpdates, sendTelegramRequest } from '../shared/telegram';
import { DispatchMediaType, MediaStatus } from '@prisma/client';
import { scheduleDueDispatchTasks } from '../scheduler/dispatch-scheduler';
import {
  GRAMJS_FORWARD_TARGET_CHAT_ID,
  GRAMJS_UPLOAD_WORKERS,
  RELAY_UPLOAD_GRAMJS_THRESHOLD_MB,
  RELAY_UPLOAD_PROGRESS_LOG_MILESTONES,
  RELAY_UPLOAD_QUEUE_CONCURRENCY,
  TYPEA_FAIL_ON_FILE_MISSING,
  TYPEA_INGEST_LEASE_MS,
  TYPEA_INGEST_MAX_RETRIES,
} from '../config/env';
import { TYPEA_INGEST_ERROR_CODE, TYPEA_INGEST_FINAL_REASON } from '../shared/metrics';

const PROGRESS_TTL_SECONDS = 24 * 60 * 60;
const PROGRESS_WRITE_INTERVAL_MS = 5000;

async function readGroupMessageTxt(localPath: string) {
  try {
    const messageTxtPath = join(dirname(localPath), 'message.txt');
    const content = await readFile(messageTxtPath, 'utf8');
    const trimmed = content.replace(/^\uFEFF/, '').trim();
    return trimmed || null;
  } catch {
    return null;
  }
}

function createProgressMilestoneLogger(params: {
  traceId: string;
  mediaAssetId: string;
  relayChannelId: string;
  stage: 'gramjs_progress' | 'upload_progress_percent';
  extra?: Record<string, unknown>;
}) {
  const loggedMilestones = new Set<number>();

  return (percentRaw: number) => {
    const percent = Math.max(0, Math.min(100, Number(percentRaw.toFixed(2))));

    for (const milestone of RELAY_UPLOAD_PROGRESS_LOG_MILESTONES) {
      if (percent >= milestone && !loggedMilestones.has(milestone)) {
        logger.info('[q_relay_upload] 上传进度里程碑', {
          traceId: params.traceId,
          stage: params.stage,
          mediaAssetId: params.mediaAssetId,
          relayChannelId: params.relayChannelId,
          progress: milestone,
          ...(params.extra || {}),
        });
        loggedMilestones.add(milestone);
      }
    }
  };
}

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
  uploadMethod: 'gramjs_sendVideo' | 'sendVideo' | 'sendPhoto' | 'recover_after_hang_up';
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

async function logCloneRelayDispatchLinkSnapshot(context: {
  traceId: string;
  mediaAssetId: string;
  relayChannelId: string;
  uploadMethod: 'gramjs_sendVideo' | 'sendVideo' | 'sendPhoto' | 'recover_after_hang_up';
  stage: 'before_dispatch_schedule' | 'after_dispatch_schedule';
  scheduleResult?: {
    queuedCount: number;
    autoBypassCount: number;
    blockedCount: number;
  };
}) {
  try {
    const mediaAssetId = BigInt(context.mediaAssetId);
    const asset = await prisma.mediaAsset.findUnique({
      where: { id: mediaAssetId },
      select: {
        id: true,
        channelId: true,
        status: true,
        telegramFileId: true,
        relayMessageId: true,
        sourceMeta: true,
        dispatchTasks: {
          select: {
            id: true,
            status: true,
            mediaAssetId: true,
            groupKey: true,
            retryCount: true,
            maxRetries: true,
            telegramMessageId: true,
            nextRunAt: true,
            finishedAt: true,
          },
          orderBy: [{ id: 'desc' }],
          take: 8,
        },
      },
    });

    if (!asset) {
      logger.warn('[clone][trace] relay->dispatch snapshot skipped: mediaAsset missing', {
        traceId: context.traceId,
        stage: context.stage,
        mediaAssetId: context.mediaAssetId,
      });
      return;
    }

    const sourceMeta = asset.sourceMeta && typeof asset.sourceMeta === 'object'
      ? (asset.sourceMeta as Record<string, unknown>)
      : {};

    const isCloneCrawlAsset = sourceMeta.isCloneCrawlAsset === true;
    const sourceMessageId = typeof sourceMeta.sourceMessageId === 'string' ? sourceMeta.sourceMessageId : null;
    const sourceChannelUsernameRaw = typeof sourceMeta.sourceChannelUsername === 'string'
      ? sourceMeta.sourceChannelUsername
      : null;
    const sourceChannelUsername = sourceChannelUsernameRaw?.trim().replace(/^@+/, '').toLowerCase() || null;

    let cloneItemRef: { itemId: string; runId: string; taskId: string; downloadStatus: string } | null = null;

    if (isCloneCrawlAsset && sourceMessageId && sourceChannelUsername) {
      try {
        const cloneItem = await prisma.cloneCrawlItem.findFirst({
          where: {
            channelUsername: sourceChannelUsername,
            messageId: BigInt(sourceMessageId),
          },
          select: {
            id: true,
            runId: true,
            taskId: true,
            downloadStatus: true,
          },
          orderBy: { id: 'desc' },
        });

        if (cloneItem) {
          cloneItemRef = {
            itemId: cloneItem.id.toString(),
            runId: cloneItem.runId.toString(),
            taskId: cloneItem.taskId.toString(),
            downloadStatus: cloneItem.downloadStatus,
          };
        }
      } catch {
        // best effort
      }
    }

    logger.info('[clone][trace] item->relay_uploaded->dispatch snapshot', {
      traceId: context.traceId,
      stage: context.stage,
      relayChannelId: context.relayChannelId,
      uploadMethod: context.uploadMethod,
      mediaAssetId: asset.id.toString(),
      mediaAssetChannelId: asset.channelId.toString(),
      relayUploaded: asset.status,
      relayMessageId: asset.relayMessageId?.toString?.() ?? null,
      telegramFileIdPresent: Boolean(asset.telegramFileId),
      isCloneCrawlAsset,
      sourceChannelUsername,
      sourceMessageId,
      cloneItemRef,
      dispatchTaskCount: asset.dispatchTasks.length,
      dispatchTasks: asset.dispatchTasks.map((t) => ({
        dispatchTaskId: t.id.toString(),
        mediaAssetId: t.mediaAssetId.toString(),
        status: t.status,
        groupKey: t.groupKey,
        retryCount: t.retryCount,
        maxRetries: t.maxRetries,
        telegramMessageId: t.telegramMessageId?.toString?.() ?? null,
        nextRunAt: t.nextRunAt?.toISOString?.() ?? null,
        finishedAt: t.finishedAt?.toISOString?.() ?? null,
      })),
      scheduleResult: context.scheduleResult ?? null,
    });
  } catch (error) {
    logger.warn('[clone][trace] relay->dispatch snapshot failed', {
      traceId: context.traceId,
      stage: context.stage,
      mediaAssetId: context.mediaAssetId,
      reason: error instanceof Error ? error.message : String(error),
    });
  }
}

async function triggerDispatchAfterRelayUpload(context: {
  traceId: string;
  mediaAssetId: string;
  relayChannelId: string;
  uploadMethod: 'gramjs_sendVideo' | 'sendVideo' | 'sendPhoto' | 'recover_after_hang_up';
}) {
  try {
    await logCloneRelayDispatchLinkSnapshot({
      ...context,
      stage: 'before_dispatch_schedule',
    });

    const result = await scheduleDueDispatchTasks();

    await logCloneRelayDispatchLinkSnapshot({
      ...context,
      stage: 'after_dispatch_schedule',
      scheduleResult: result,
    });

    logger.info('[q_relay_upload] 上传成功后触发一次分发调度', {
      traceId: context.traceId,
      stage: 'trigger_dispatch_after_relay_upload',
      mediaAssetId: context.mediaAssetId,
      relayChannelId: context.relayChannelId,
      uploadMethod: context.uploadMethod,
      queuedCount: result.queuedCount,
      autoBypassCount: result.autoBypassCount,
      blockedCount: result.blockedCount,
    });
  } catch (error) {
    logger.warn('[q_relay_upload] 上传成功后触发分发调度失败（不阻断当前任务）', {
      traceId: context.traceId,
      stage: 'trigger_dispatch_after_relay_upload_failed',
      mediaAssetId: context.mediaAssetId,
      relayChannelId: context.relayChannelId,
      uploadMethod: context.uploadMethod,
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

    if (job.name === 'relay-upload-grouped') {
      const relayChannelIdRaw = job.data.relayChannelId as string | undefined;
      const groupKey = job.data.groupKey as string | undefined;
      const mediaAssetIds = Array.isArray(job.data.mediaAssetIds)
        ? (job.data.mediaAssetIds as string[]).filter((v) => typeof v === 'string' && v.trim())
        : [];

      if (!relayChannelIdRaw || mediaAssetIds.length === 0) {
        throw new Error('grouped relay upload payload invalid');
      }

      let enqueued = 0;
      for (const mediaAssetIdRaw of mediaAssetIds) {
        const childJobId = `relay-upload-${mediaAssetIdRaw}`;
        const existing = await prisma.mediaAsset.findUnique({
          where: { id: BigInt(mediaAssetIdRaw) },
          select: { id: true, status: true, telegramFileId: true, relayMessageId: true },
        });

        if (!existing || existing.status === MediaStatus.relay_uploaded || existing.telegramFileId || existing.relayMessageId) {
          continue;
        }

        const existingJob = await relayUploadQueue.getJob(childJobId);
        if (existingJob) {
          const state = await existingJob.getState();
          if (state !== 'failed') continue;
          await existingJob.remove();
        }

        await relayUploadQueue.add(
          'relay-upload',
          {
            mediaAssetId: mediaAssetIdRaw,
            relayChannelId: relayChannelIdRaw,
            groupKey,
            groupDispatchMode: 'grouped_child',
          },
          {
            jobId: childJobId,
            removeOnComplete: true,
            removeOnFail: 200,
          },
        );
        enqueued += 1;
      }

      logger.info('[q_relay_upload] grouped task expanded', {
        jobId: String(job.id),
        groupKey: groupKey ?? null,
        groupSize: mediaAssetIds.length,
        enqueued,
      });

      return { ok: true, grouped: true, enqueued };
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

    const sourceMetaForIdempotent =
      mediaAsset.sourceMeta && typeof mediaAsset.sourceMeta === 'object'
        ? (mediaAsset.sourceMeta as Record<string, unknown>)
        : {};
    const sourceMessageIdForIdempotent =
      typeof sourceMetaForIdempotent.sourceMessageId === 'string'
        ? sourceMetaForIdempotent.sourceMessageId
        : null;
    const isCloneCrawlAssetForIdempotent = sourceMetaForIdempotent.isCloneCrawlAsset === true;

    if (isCloneCrawlAssetForIdempotent && sourceMessageIdForIdempotent) {
      const uploadedSameSource = await prisma.mediaAsset.findFirst({
        where: {
          channelId: mediaAsset.channelId,
          id: { not: mediaAsset.id },
          OR: [
            { status: MediaStatus.relay_uploaded },
            { telegramFileId: { not: null } },
            { relayMessageId: { not: null } },
          ],
          sourceMeta: {
            path: ['sourceMessageId'],
            equals: sourceMessageIdForIdempotent,
          },
        },
        select: {
          id: true,
          relayMessageId: true,
          telegramFileId: true,
          telegramFileUniqueId: true,
          dispatchMediaType: true,
        },
      });

      if (uploadedSameSource) {
        const ingestFinishedAt = new Date();
        await prisma.mediaAsset.update({
          where: { id: mediaAsset.id },
          data: {
            status: MediaStatus.relay_uploaded,
            relayMessageId: uploadedSameSource.relayMessageId,
            telegramFileId: uploadedSameSource.telegramFileId,
            telegramFileUniqueId: uploadedSameSource.telegramFileUniqueId,
            dispatchMediaType: uploadedSameSource.dispatchMediaType,
            ingestError: null,
            ingestFinishedAt,
            ingestDurationSec: calcIngestDurationSec(ingestStartedAt, ingestFinishedAt),
            sourceMeta: {
              ...sourceMetaForIdempotent,
              ingestLeaseUntil: null,
              ingestLastHeartbeatAt: new Date().toISOString(),
              ingestWorkerJobId: null,
              ingestStage: 'done',
              uploadIdempotentSkip: true,
              uploadIdempotentReason: 'same_source_message_already_uploaded',
              uploadIdempotentFromMediaAssetId: uploadedSameSource.id.toString(),
            },
          } as any,
        });

        await removeUploadedSourceFile(mediaAsset.localPath, {
          traceId,
          mediaAssetId: mediaAssetIdRaw,
          relayChannelId: relayChannelIdRaw,
          uploadMethod: 'recover_after_hang_up',
        });

        logger.info('[q_relay_upload] 发送幂等命中，复用已上传 sourceMessageId', {
          traceId,
          mediaAssetId: mediaAssetIdRaw,
          sourceMessageId: sourceMessageIdForIdempotent,
          uploadedFromMediaAssetId: uploadedSameSource.id.toString(),
        });

        return {
          ok: true,
          skipped: true,
          reason: 'idempotent_same_source_message_uploaded',
          mediaAssetId: mediaAssetIdRaw,
        };
      }
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
    const fileExt = extname(mediaAsset.localPath).toLowerCase();
    const fileSize = mediaAsset.fileSize ? Number(mediaAsset.fileSize) : undefined;
    const sourceMeta =
      mediaAsset.sourceMeta && typeof mediaAsset.sourceMeta === 'object'
        ? (mediaAsset.sourceMeta as Record<string, unknown>)
        : {};
    const groupedMessageTxt = await readGroupMessageTxt(mediaAsset.localPath);
    const sourceMetaWithGroupTxt = groupedMessageTxt
      ? {
          ...sourceMeta,
          messageTxt: groupedMessageTxt,
          txtContent: groupedMessageTxt,
        }
      : sourceMeta;
    const mimeType = typeof sourceMeta.mimeType === 'string' ? sourceMeta.mimeType.toLowerCase() : '';

    const isImage = mimeType.startsWith('image/') || ['.jpg', '.jpeg', '.png', '.webp'].includes(fileExt);
    const isVideo = mimeType.startsWith('video/') || ['.mp4', '.mov', '.mkv', '.webm'].includes(fileExt);

    if (!isImage && !isVideo) {
      throw new Error(`中转上传不支持的媒体类型: ext=${fileExt}, mime=${mimeType || 'unknown'}`);
    }
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
      const logGramjsProgressMilestone = createProgressMilestoneLogger({
        traceId,
        mediaAssetId: mediaAssetIdRaw,
        relayChannelId: relayChannelIdRaw,
        stage: 'gramjs_progress',
        extra: {
          uploadMethod: 'gramjs_sendVideo',
        },
      });

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
            logGramjsProgressMilestone(percent);

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

      const forwardVideoFileId = forwardResult.videoFileId;
      const forwardVideoFileUniqueId = forwardResult.videoFileUniqueId ?? null;
      const forwardPhotoFileId = forwardResult.photoFileId;
      const forwardPhotoFileUniqueId = forwardResult.photoFileUniqueId ?? null;

      const resolvedTelegramFileId = isImage ? forwardPhotoFileId : forwardVideoFileId;
      const resolvedTelegramFileUniqueId = isImage ? forwardPhotoFileUniqueId : forwardVideoFileUniqueId;

      if (!resolvedTelegramFileId) {
        const ingestFinishedAt = new Date();
        await prisma.mediaAsset.update({
          where: { id: mediaAssetId },
          data: {
            status: MediaStatus.failed,
            relayMessageId: BigInt(gramjsMessageId),
            ingestError: isImage
              ? 'GramJS 上传成功但 forwardMessage 未返回 photo_file_id'
              : 'GramJS 上传成功但 forwardMessage 未返回 video_file_id（疑似被识别为 document）',
            ingestFinishedAt,
            ingestDurationSec: calcIngestDurationSec(ingestStartedAt, ingestFinishedAt),
          } as any,
        });

        throw new Error(
          isImage
            ? 'GramJS 上传成功但 forwardMessage 未返回 photo_file_id'
            : 'GramJS 上传成功但 forwardMessage 未返回 video_file_id',
        );
      }

      const ingestFinishedAt = new Date();
      await prisma.mediaAsset.update({
        where: { id: mediaAssetId },
        data: {
          status: MediaStatus.relay_uploaded,
          relayMessageId: BigInt(gramjsMessageId),
          telegramFileId: resolvedTelegramFileId,
          telegramFileUniqueId: resolvedTelegramFileUniqueId,
          dispatchMediaType: isImage ? DispatchMediaType.photo : DispatchMediaType.video,
          ingestError: null,
          durationSec: normalizedDurationSec,
          ingestFinishedAt,
          ingestDurationSec: calcIngestDurationSec(ingestStartedAt, ingestFinishedAt),
          archivePath: null,
          sourceMeta: {
            ...sourceMetaWithGroupTxt,
            relayBotId: relayChannel.bot.id.toString(),
            relayResolvedMediaType: isImage ? 'photo' : 'video',
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

      await triggerDispatchAfterRelayUpload({
        traceId,
        mediaAssetId: mediaAssetIdRaw,
        relayChannelId: relayChannelIdRaw,
        uploadMethod: 'gramjs_sendVideo',
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
    let lastProgressWriteAt = 0;
    const uploadMethod = isImage ? 'sendPhoto' : 'sendVideo';
    const uploadField = isImage ? 'photo' : 'video';

    const logUploadProgressMilestone = createProgressMilestoneLogger({
      traceId,
      mediaAssetId: mediaAssetIdRaw,
      relayChannelId: relayChannelIdRaw,
      stage: 'upload_progress_percent',
      extra: {
        uploadMethod,
      },
    });

    fileStream.on('data', (chunk) => {
      streamedBytes += chunk.length;
      const now = Date.now();
      const progress = fileSize ? (streamedBytes / fileSize) * 100 : 0;
      logUploadProgressMilestone(progress);

      if (now - lastProgressWriteAt >= PROGRESS_WRITE_INTERVAL_MS) {
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

    formData.append(uploadField, streamWithProgress, {
      filename: fileName,
      knownLength: fileSize,
    });
    if (!isImage && thumbnailPath) {
      formData.append('thumbnail', createReadStream(thumbnailPath), {
        filename: 'thumb.jpg',
      });
    }
    if (!isImage) {
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
      uploadMethod,
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
        method: isImage ? 'sendPhoto' : 'sendVideo',
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
              dispatchMediaType: isImage ? DispatchMediaType.photo : DispatchMediaType.video,
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

          await triggerDispatchAfterRelayUpload({
            traceId,
            mediaAssetId: mediaAssetIdRaw,
            relayChannelId: relayChannelIdRaw,
            uploadMethod: 'recover_after_hang_up',
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

    const directFileId = isImage ? sendResult.photoFileId : sendResult.videoFileId;
    const directFileUniqueId = isImage
      ? (sendResult.photoFileUniqueId ?? null)
      : (sendResult.videoFileUniqueId ?? null);

    if (directFileId) {
      const ingestFinishedAt = new Date();
      await prisma.mediaAsset.update({
        where: { id: mediaAssetId },
        data: {
          status: MediaStatus.relay_uploaded,
          relayMessageId: BigInt(sendResult.messageId),
          telegramFileId: directFileId,
          telegramFileUniqueId: directFileUniqueId,
          dispatchMediaType: isImage ? DispatchMediaType.photo : DispatchMediaType.video,
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
            relayResolvedMediaType: isImage ? 'photo' : 'video',
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
        uploadMethod,
      });

      await writeProgress({
        mediaAssetId: mediaAssetIdRaw,
        streamedBytes: fileSize ?? streamedBytes,
        totalBytes: fileSize,
        progress: 100,
      });

      await triggerDispatchAfterRelayUpload({
        traceId,
        mediaAssetId: mediaAssetIdRaw,
        relayChannelId: relayChannelIdRaw,
        uploadMethod,
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
    : { raw: err, string: toReadableErrorSummary(err), json: errorJson };

  logError('[q_relay_upload] 任务失败', {
    jobId: job?.id ? String(job.id) : null,
    mediaAssetId: mediaAssetIdRaw ?? null,
    errorSummary: toReadableErrorSummary(err),
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

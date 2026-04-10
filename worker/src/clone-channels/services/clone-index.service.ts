import { prisma } from '../../infra/prisma';
import { cloneRetryQueue, cloneVideoDownloadQueue } from '../../infra/redis';
import { logger } from '../../logger';
import { withClient } from './clone-session.service';
import {
  CloneChannelIndexJob,
  CloneContentType,
  CloneRetryReason,
  IndexedMessageDTO,
} from '../types/clone-queue.types';
import { CLONE_RETRY_MAX } from '../constants/clone-queue.constants';

function classifyRetryReason(err: unknown): CloneRetryReason {
  const message = err instanceof Error ? err.message.toLowerCase() : '';
  if (message.includes('floodwait') || message.includes('flood_wait')) return 'flood_wait';
  if (message.includes('timeout') || message.includes('network') || message.includes('socket')) return 'network_timeout';
  if (message.includes('auth_invalid') || message.includes('auth')) return 'auth_invalid';
  if (message.includes('channel') && message.includes('unreachable')) return 'channel_unreachable';
  if (message.includes('bot_method_invalid')) return 'auth_invalid';
  return 'index_unknown_error';
}

function normalizeChannelUsername(raw: string) {
  return raw.trim().replace(/^@+/, '').toLowerCase();
}

function toDbChannelUsername(normalized: string) {
  return `@${normalized}`;
}

function parseContentTypes(raw: string[] | CloneContentType[] | undefined): CloneContentType[] {
  const allowed: CloneContentType[] = ['text', 'image', 'video'];
  if (!raw || raw.length === 0) return allowed;
  const normalized = raw
    .map((t) => String(t).toLowerCase())
    .filter((t): t is CloneContentType => allowed.includes(t as CloneContentType));
  return normalized.length ? Array.from(new Set(normalized)) : allowed;
}

async function fetchIncrementalMessages(params: {
  channelUsername: string;
  lastFetchedMessageId?: bigint | null;
  recentLimit: number;
  contentTypes: CloneContentType[];
}): Promise<IndexedMessageDTO[]> {
  const { channelUsername, lastFetchedMessageId, recentLimit, contentTypes } = params;

  const limit = Math.max(1, Math.min(1000, recentLimit || 100));
  logger.info('[clone][索引/Index] 开始拉取 Telegram 消息 / start fetch telegram messages', {
    channelUsername,
    lastFetchedMessageId: lastFetchedMessageId?.toString() ?? null,
    limit,
    contentTypes,
  });

  const messages = await withClient({ timeoutMs: 120_000, accountType: 'user' }, async (client) => {
    const entity = await (client as any).getEntity(channelUsername);
    const list = await (client as any).getMessages(entity, {
      limit,
      minId: lastFetchedMessageId ? Number(lastFetchedMessageId) : undefined,
    });
    return Array.isArray(list) ? list : [];
  });

  const picked: IndexedMessageDTO[] = [];

  let skippedByNoId = 0;
  let skippedByType = 0;
  let pickedVideo = 0;
  let pickedImage = 0;
  let pickedText = 0;

  for (const msg of messages) {
    const messageIdRaw = (msg as any)?.id;
    if (!Number.isFinite(messageIdRaw)) {
      skippedByNoId += 1;
      continue;
    }

    const messageText = ((msg as any)?.message ?? '') as string;
    const media = (msg as any)?.media;
    const document = (media as any)?.document;

    const mimeType = typeof document?.mimeType === 'string' ? document.mimeType : undefined;
    const maybeSize = Number(document?.size);
    const fileSize = Number.isFinite(maybeSize) && maybeSize > 0 ? BigInt(Math.floor(maybeSize)) : undefined;

    const hasVideo = Boolean(
      mimeType?.toLowerCase().startsWith('video/') ||
      ((document?.attributes ?? []) as any[]).some((attr) => String(attr?.className ?? '').toLowerCase().includes('video')),
    );

    const hasImage = Boolean(mimeType?.toLowerCase().startsWith('image/'));
    const hasText = Boolean(messageText && messageText.trim().length > 0);

    let include = false;
    if (hasVideo && contentTypes.includes('video')) include = true;
    else if (hasImage && contentTypes.includes('image')) include = true;
    else if (hasText && contentTypes.includes('text')) include = true;

    if (!include) {
      skippedByType += 1;
      continue;
    }

    if (hasVideo) pickedVideo += 1;
    else if (hasImage) pickedImage += 1;
    else if (hasText) pickedText += 1;

    picked.push({
      messageId: BigInt(Math.floor(messageIdRaw)),
      messageDate: (msg as any)?.date ? new Date((msg as any).date) : undefined,
      messageText,
      hasVideo,
      fileSize,
      mimeType,
      mediaRef: `tg://message/${channelUsername}/${messageIdRaw}`,
    });
  }

  logger.info('[clone][索引/Index] Telegram 消息过滤完成 / telegram message filtering done', {
    channelUsername,
    fetchedRaw: messages.length,
    picked: picked.length,
    pickedVideo,
    pickedImage,
    pickedText,
    skippedByNoId,
    skippedByType,
  });

  return picked;
}

async function upsertIndexedItems(params: {
  taskId: bigint;
  runId: bigint;
  channelUsername: string;
  items: IndexedMessageDTO[];
  crawlMode: 'index_only' | 'index_and_download';
  targetPath: string;
}): Promise<{ inserted: number; deduped: number; queuedDownloads: number; maxMessageId?: bigint }> {
  let inserted = 0;
  let deduped = 0;
  let queuedDownloads = 0;
  let maxMessageId: bigint | undefined;

  for (const row of params.items) {
    if (!maxMessageId || row.messageId > maxMessageId) maxMessageId = row.messageId;

    const exists = await prisma.cloneCrawlItem.findUnique({
      where: {
        channelUsername_messageId: {
          channelUsername: params.channelUsername,
          messageId: row.messageId,
        },
      },
      select: { id: true },
    });

    if (exists) {
      deduped += 1;
      continue;
    }

    const item = await prisma.cloneCrawlItem.create({
      data: {
        taskId: params.taskId,
        runId: params.runId,
        channelUsername: params.channelUsername,
        messageId: row.messageId,
        messageDate: row.messageDate,
        messageText: row.messageText,
        hasVideo: row.hasVideo,
        fileSize: row.fileSize,
        mimeType: row.mimeType,
        localPath: params.targetPath,
        downloadStatus:
          params.crawlMode === 'index_and_download' && row.hasVideo ? 'queued' : 'none',
      },
    });

    inserted += 1;

    if (params.crawlMode === 'index_and_download' && row.hasVideo) {
      await cloneVideoDownloadQueue.add(
        'clone-video-download',
        {
          taskId: params.taskId.toString(),
          runId: params.runId.toString(),
          itemId: item.id.toString(),
          channelUsername: params.channelUsername,
          mediaRef: row.mediaRef
            ? {
                kind: 'tg_message',
                channelUsername: params.channelUsername,
                messageId: row.messageId.toString(),
              }
            : undefined,
          expectedFileSize: row.fileSize ? row.fileSize.toString() : undefined,
          targetPath: params.targetPath,
          priority:
            row.fileSize && row.fileSize > BigInt(1024 * 1024 * 1024)
              ? 'large'
              : row.fileSize && row.fileSize < BigInt(200 * 1024 * 1024)
                ? 'small'
                : 'medium',
          enqueuedAt: new Date().toISOString(),
        },
        { removeOnComplete: true, removeOnFail: 100 },
      );

      queuedDownloads += 1;
      logger.info('[clone][索引/Index] 视频下载任务已入队 / video download job enqueued', {
        taskId: params.taskId.toString(),
        runId: params.runId.toString(),
        channelUsername: params.channelUsername,
        itemId: item.id.toString(),
        queue: cloneVideoDownloadQueue.name,
      });
    }
  }

  logger.info('[clone][索引/Index] 明细入库完成 / indexed items persisted', {
    taskId: params.taskId.toString(),
    runId: params.runId.toString(),
    channelUsername: params.channelUsername,
    inputCount: params.items.length,
    inserted,
    deduped,
    queuedDownloads,
    maxMessageId: maxMessageId?.toString() ?? null,
  });

  return { inserted, deduped, queuedDownloads, maxMessageId };
}

export async function processCloneChannelIndex(job: CloneChannelIndexJob) {
  const runId = BigInt(job.runId);
  const taskId = BigInt(job.taskId);
  const channelUsername = normalizeChannelUsername(job.channelUsername);
  const dbChannelUsername = toDbChannelUsername(channelUsername);

  logger.info('[clone][索引/Index] 开始处理频道索引 / start channel indexing', {
    taskId: job.taskId,
    runId: job.runId,
    channelUsername,
    retryCount: job.retryCount ?? 0,
  });

  try {
    const task = await prisma.cloneCrawlTask.findUnique({
      where: { id: taskId },
      select: {
        id: true,
        crawlMode: true,
        targetPath: true,
        recentLimit: true,
        contentTypes: true,
      },
    });

    if (!task) {
      logger.warn('[clone][索引/Index] 任务不存在，跳过 / task not found, skip indexing', {
        taskId: job.taskId,
        runId: job.runId,
        channelUsername,
      });
      return;
    }

    const channel = await prisma.cloneCrawlTaskChannel.findUnique({
      where: {
        taskId_channelUsername: {
          taskId,
          channelUsername: dbChannelUsername,
        },
      },
      select: {
        id: true,
        channelUsername: true,
        channelAccessStatus: true,
        lastFetchedMessageId: true,
      },
    });

    if (!channel) {
      throw new Error(`channel_unreachable: task channel not found for ${dbChannelUsername}`);
    }

    if (channel.channelAccessStatus !== 'ok') {
      throw new Error(`channel_unreachable: channel access status is ${channel.channelAccessStatus}`);
    }

    const lastFetchedMessageId = job.lastFetchedMessageId
      ? BigInt(job.lastFetchedMessageId)
      : channel?.lastFetchedMessageId ?? null;

    const messages = await fetchIncrementalMessages({
      channelUsername,
      lastFetchedMessageId,
      recentLimit: job.recentLimit ?? task.recentLimit,
      contentTypes: parseContentTypes(job.contentTypes ?? (task.contentTypes as string[])),
    });

    const { inserted, deduped, queuedDownloads, maxMessageId } = await upsertIndexedItems({
      taskId,
      runId,
      channelUsername,
      items: messages,
      crawlMode: task.crawlMode,
      targetPath: task.targetPath,
    });

    if (maxMessageId && channel) {
      await prisma.cloneCrawlTaskChannel.update({
        where: { id: channel.id },
        data: {
          lastFetchedMessageId: maxMessageId,
          lastRunAt: new Date(),
          lastErrorCode: null,
          lastErrorMessage: null,
        },
      });
    }

    const currentRun = await prisma.cloneCrawlRun.findUnique({
      where: { id: runId },
      select: { id: true, channelTotal: true, channelSuccess: true, channelFailed: true },
    });

    const nextChannelSuccess = (currentRun?.channelSuccess ?? 0) + 1;
    const channelTotal = currentRun?.channelTotal ?? 0;
    const shouldFinish = channelTotal > 0 && nextChannelSuccess + (currentRun?.channelFailed ?? 0) >= channelTotal;

    await prisma.cloneCrawlRun.update({
      where: { id: runId },
      data: {
        status: shouldFinish ? 'success' : 'running',
        indexedCount: { increment: inserted },
        dedupCount: { increment: deduped },
        channelSuccess: { increment: 1 },
        finishedAt: shouldFinish ? new Date() : undefined,
        downloadQueued:
          task.crawlMode === 'index_and_download'
            ? { increment: queuedDownloads }
            : undefined,
      },
    });

    logger.info('[clone][索引/Index] 频道索引完成 / channel indexing done', {
      taskId: job.taskId,
      runId: job.runId,
      channelUsername,
      crawlMode: task.crawlMode,
      fetched: messages.length,
      inserted,
      deduped,
      maxMessageId: maxMessageId?.toString() ?? null,
    });
  } catch (err) {
    const reason = classifyRetryReason(err);
    const errorMessage = err instanceof Error ? err.message : String(err);
    const currentRetryCount = job.retryCount ?? 0;
    const nextRetryCount = currentRetryCount + 1;

    const retryAfterSec = (() => {
      const m = errorMessage.match(/flood[_\s]?wait[_\s]?(\d+)/i);
      if (!m) return undefined;
      const n = Number(m[1]);
      return Number.isFinite(n) && n > 0 ? Math.floor(n) : undefined;
    })();

    const nonRetryable = reason === 'auth_invalid' || reason === 'channel_unreachable';
    const shouldRetry = !nonRetryable && nextRetryCount <= CLONE_RETRY_MAX;

    if (shouldRetry) {
      await cloneRetryQueue.add(
        'clone-index-retry',
        {
          queue: 'index',
          reason,
          retryCount: currentRetryCount,
          retryAfterSec,
          payload: {
            ...job,
            channelUsername,
            retryCount: nextRetryCount,
          },
          firstFailedAt: new Date().toISOString(),
          lastErrorMessage: errorMessage,
        },
        { removeOnComplete: true, removeOnFail: 100 },
      );
    }

    await prisma.cloneCrawlTaskChannel.updateMany({
      where: {
        taskId,
        channelUsername: dbChannelUsername,
      },
      data: {
        lastRunAt: new Date(),
        lastErrorCode: reason,
        lastErrorMessage: errorMessage,
      },
    });

    if (!shouldRetry) {
      await prisma.cloneCrawlRun.updateMany({
        where: { id: runId },
        data: {
          status: 'failed',
          channelFailed: { increment: 1 },
        },
      });
    }

    logger.warn('[clone][索引/Index] 索引失败 / indexing failed', {
      taskId: job.taskId,
      runId: job.runId,
      channelUsername,
      reason,
      shouldRetry,
      currentRetryCount,
      nextRetryCount,
      retryAfterSec,
      queue: shouldRetry ? cloneRetryQueue.name : null,
      error: errorMessage,
    });
  }
}

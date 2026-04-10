import { access, copyFile, mkdir, rename, rm, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { constants as fsConstants } from 'node:fs';
import { prisma } from '../../infra/prisma';
import { cloneRetryQueue } from '../../infra/redis';
import {
  CLONE_DOWNLOAD_CHUNK_SIZE_BYTES,
  CLONE_DOWNLOAD_TIMEOUT_MS,
  CLONE_DOWNLOAD_VALIDATE_FFPROBE,
} from '../constants/clone-queue.constants';
import { logger } from '../../logger';
import { CloneMediaRef, CloneRetryReason, CloneVideoDownloadJob } from '../types/clone-queue.types';
import { checkDownloadGuards, recordGuardTriggered } from './clone-guard.service';
import { withClient } from './clone-session.service';

function classifyRetryReason(err: unknown): CloneRetryReason {
  const message = err instanceof Error ? err.message.toLowerCase() : '';
  if (message.includes('floodwait') || message.includes('flood_wait')) return 'flood_wait';
  if (message.includes('timeout') || message.includes('network') || message.includes('socket')) return 'network_timeout';
  if (message.includes('auth_invalid') || message.includes('auth')) return 'auth_invalid';
  if (message.includes('file_too_large')) return 'file_too_large';
  return 'download_unknown_error';
}

function sanitizeFileName(name: string) {
  return name.replace(/[<>:"/\\|?*\u0000-\u001F]/g, '_').replace(/\s+/g, ' ').trim();
}

async function ensureDir(targetDir: string) {
  await mkdir(targetDir, { recursive: true });
}

async function pathExists(filePath: string) {
  try {
    await access(filePath, fsConstants.F_OK);
    return true;
  } catch {
    return false;
  }
}

async function finalizeDownloadedFile(params: {
  tempFilePath: string;
  finalDir: string;
  finalFileName: string;
}): Promise<{ finalPath: string }> {
  await ensureDir(params.finalDir);

  const base = sanitizeFileName(params.finalFileName || 'video.mp4');
  const ext = path.extname(base);
  const stem = ext ? base.slice(0, -ext.length) : base;

  let candidate = path.join(params.finalDir, base);
  let index = 1;

  while (await pathExists(candidate)) {
    candidate = path.join(params.finalDir, `${stem}-${index}${ext}`);
    index += 1;
  }

  await rename(params.tempFilePath, candidate);
  return { finalPath: candidate };
}

async function validateDownloadedFile(params: {
  filePath: string;
  expectedSize?: bigint;
  enableFfprobe?: boolean;
}): Promise<{ ok: true } | { ok: false; reason: string }> {
  try {
    const fsStat = await stat(params.filePath);
    if (!fsStat.isFile()) return { ok: false, reason: 'not_a_file' };
    if (fsStat.size <= 0) return { ok: false, reason: 'empty_file' };

    if (params.expectedSize && params.expectedSize > BigInt(0)) {
      const expected = Number(params.expectedSize);
      if (Number.isFinite(expected) && expected > 0 && Math.abs(fsStat.size - expected) > 1024) {
        return { ok: false, reason: `size_mismatch: expected=${expected}, actual=${fsStat.size}` };
      }
    }

    if (params.enableFfprobe) {
      // 预留扩展：接入 ffprobe 校验
    }

    return { ok: true };
  } catch (err) {
    return { ok: false, reason: err instanceof Error ? err.message : String(err) };
  }
}

function parseMediaRef(mediaRef: CloneMediaRef | undefined, fallback: {
  channelUsername: string;
  messageId: bigint;
}): CloneMediaRef {
  if (!mediaRef) {
    return {
      kind: 'tg_message',
      channelUsername: fallback.channelUsername,
      messageId: fallback.messageId.toString(),
    };
  }

  if (mediaRef.kind === 'tg_message') {
    return {
      kind: 'tg_message',
      channelUsername: mediaRef.channelUsername.trim().replace(/^@+/, '').toLowerCase(),
      messageId: mediaRef.messageId,
    };
  }

  return mediaRef;
}

function getResumeTempPath(targetDir: string, fileName: string) {
  return path.join(targetDir, `${sanitizeFileName(fileName)}.part`);
}

function formatMbps(bytes: number, elapsedMs: number) {
  if (elapsedMs <= 0) return 0;
  return (bytes * 8) / (elapsedMs / 1000) / 1024 / 1024;
}

function startProgressLogger(params: {
  tempFilePath: string;
  channelUsername: string;
  messageId: number;
  expectedSize?: bigint;
}) {
  const startedAt = Date.now();
  let lastLoggedSize = -1;

  const timer = setInterval(async () => {
    try {
      const fsStat = await stat(params.tempFilePath);
      const currentSize = Math.max(0, fsStat.size);
      if (currentSize === lastLoggedSize) return;
      lastLoggedSize = currentSize;

      const elapsedMs = Date.now() - startedAt;
      const speedMbps = formatMbps(currentSize, elapsedMs);
      const expected = params.expectedSize ? Number(params.expectedSize) : 0;
      const progressPct = expected > 0 ? Math.min(100, (currentSize / expected) * 100) : null;

      logger.info('[clone][下载/Download] 下载进度 / download progress', {
        channelUsername: params.channelUsername,
        messageId: params.messageId,
        downloadedBytes: currentSize,
        expectedBytes: expected > 0 ? expected : null,
        progressPct: progressPct !== null ? Number(progressPct.toFixed(2)) : null,
        speedMbps: Number(speedMbps.toFixed(2)),
        elapsedMs,
      });
    } catch {
      // ignore stat errors while file is not ready yet
    }
  }, 3000);

  return () => clearInterval(timer);
}

async function downloadMediaToTempFile(params: {
  mediaRef: CloneMediaRef;
  targetDir: string;
  fileName: string;
  timeoutMs: number;
}): Promise<{ tempFilePath: string; downloadedBytes: bigint; mimeType?: string }> {
  await ensureDir(params.targetDir);

  const tempFilePath = getResumeTempPath(params.targetDir, params.fileName);

  if (params.mediaRef.kind === 'local_file') {
    logger.info('[clone][下载/Download] 走本地文件复制分支 / using local_file mediaRef', {
      sourcePath: params.mediaRef.filePath,
      tempFilePath,
    });
    await copyFile(params.mediaRef.filePath, tempFilePath);
  } else if (params.mediaRef.kind === 'tg_message') {
    const channelUsername = params.mediaRef.channelUsername;
    const messageId = Number(params.mediaRef.messageId);

    logger.info('[clone][下载/Download] 走 Telegram 消息下载分支 / using tg_message mediaRef', {
      channelUsername,
      messageId,
      tempFilePath,
      timeoutMs: params.timeoutMs,
      chunkSizeBytes: CLONE_DOWNLOAD_CHUNK_SIZE_BYTES,
    });

    const downloaded = await withClient({ timeoutMs: params.timeoutMs }, async (client) => {
      const c = client as unknown as {
        getEntity: (username: string) => Promise<unknown>;
        getMessages: (entity: unknown, params: { ids: number[] }) => Promise<unknown[] | unknown>;
        downloadMedia: (
          media: unknown,
          params: { outputFile: string; workers: number; partSizeKb: number },
        ) => Promise<string | Uint8Array | Buffer | null | undefined>;
      };

      const entity = await c.getEntity(channelUsername);
      const msg = await c.getMessages(entity, { ids: [messageId] });
      const one = Array.isArray(msg) ? msg[0] : msg;
      if (!one || typeof one !== 'object') {
        throw new Error(`channel_unreachable: message not found tg://${channelUsername}/${messageId}`);
      }

      const media = (one as { media?: unknown }).media;
      if (!media) {
        throw new Error(`file_missing: media missing tg://${channelUsername}/${messageId}`);
      }

      const result = await c.downloadMedia(media, {
        outputFile: tempFilePath,
        workers: 1,
        partSizeKb: Math.max(32, Math.floor(CLONE_DOWNLOAD_CHUNK_SIZE_BYTES / 1024)),
      });

      if (typeof result === 'string' && result !== tempFilePath) {
        await copyFile(result, tempFilePath);
      } else if (result instanceof Uint8Array || Buffer.isBuffer(result)) {
        await writeFile(tempFilePath, Buffer.from(result));
      }

      return true;
    });

    if (!downloaded) {
      throw new Error(`download_unknown_error: unable to download tg://${channelUsername}/${messageId}`);
    }
  } else {
    logger.warn('[clone][下载/Download] 走 opaque 占位分支 / using opaque mediaRef fallback', {
      value: params.mediaRef.value,
      tempFilePath,
    });
    const content = `mock-download from ${params.mediaRef.value} at ${new Date().toISOString()}\n`;
    await writeFile(tempFilePath, content, 'utf8');
  }

  const fsStat = await stat(tempFilePath);
  return {
    tempFilePath,
    downloadedBytes: BigInt(Math.max(0, fsStat.size)),
    mimeType: 'video/mp4',
  };
}

function toSafeBigInt(raw: string | undefined) {
  if (!raw) return undefined;
  try {
    return BigInt(raw);
  } catch {
    return undefined;
  }
}

export async function processCloneVideoDownload(job: CloneVideoDownloadJob) {
  const itemId = BigInt(job.itemId);
  const runId = BigInt(job.runId);
  const taskId = BigInt(job.taskId);

  logger.info('[clone][下载/Download] 开始处理下载任务 / start processing download job', {
    taskId: job.taskId,
    runId: job.runId,
    itemId: job.itemId,
    retryCount: job.retryCount ?? 0,
  });

  try {
    const item = await prisma.cloneCrawlItem.findUnique({ where: { id: itemId } });
    if (!item) {
      logger.warn('[clone][下载/Download] 下载项不存在，跳过 / download item not found, skip', {
        taskId: job.taskId,
        runId: job.runId,
        itemId: job.itemId,
      });
      return;
    }

    const targetPath = job.targetPath ?? item.localPath ?? process.cwd();

    await prisma.cloneCrawlItem.update({
      where: { id: itemId },
      data: {
        downloadStatus: 'downloading',
        downloadErrorCode: null,
        downloadError: null,
      },
    });

    const guard = await checkDownloadGuards({
      taskId,
      runId,
      itemId,
      channelUsername: job.channelUsername ?? item.channelUsername,
      targetPath,
      expectedFileSize: toSafeBigInt(job.expectedFileSize) ?? item.fileSize ?? undefined,
    });

    if (!guard.pass) {
      const detail = `${guard.reason}, retry after ${guard.retryDelayMs}ms`;
      logger.warn('[clone][下载/Download] 护栏未通过 / download guard rejected', {
        taskId: job.taskId,
        runId: job.runId,
        itemId: job.itemId,
        reason: guard.reason,
        retryDelayMs: guard.retryDelayMs,
        targetPath,
      });
      await recordGuardTriggered({ itemId, reason: guard.reason, detail });
      await cloneRetryQueue.add(
        'clone-download-guard-retry',
        {
          queue: 'download',
          reason: 'disk_guard_triggered',
          retryCount: job.retryCount ?? 0,
          payload: {
            ...job,
            retryCount: (job.retryCount ?? 0) + 1,
          },
          lastErrorMessage: detail,
        },
        {
          delay: guard.retryDelayMs,
          removeOnComplete: true,
          removeOnFail: 100,
        },
      );
      return;
    }

    const expectedSize = toSafeBigInt(job.expectedFileSize) ?? item.fileSize ?? undefined;
    const fileName = `${item.channelUsername}-${item.messageId.toString()}.mp4`;
    const mediaRef = parseMediaRef(job.mediaRef, {
      channelUsername: item.channelUsername,
      messageId: item.messageId,
    });

    logger.info('[clone][下载/Download] 下载参数已解析 / download params resolved', {
      taskId: job.taskId,
      runId: job.runId,
      itemId: job.itemId,
      mediaRefKind: mediaRef.kind,
      targetPath,
      expectedSize: expectedSize?.toString() ?? null,
      fileName,
    });

    const stopProgressLog = startProgressLogger({
      tempFilePath: getResumeTempPath(targetPath, fileName),
      channelUsername: mediaRef.kind === 'tg_message' ? mediaRef.channelUsername : item.channelUsername,
      messageId: mediaRef.kind === 'tg_message' ? Number(mediaRef.messageId) : Number(item.messageId),
      expectedSize,
    });

    let tempFilePath = '';
    let downloadedBytes = BigInt(0);
    let mimeType: string | undefined;

    try {
      const downloadResult = await downloadMediaToTempFile({
        mediaRef,
        targetDir: targetPath,
        fileName,
        timeoutMs: CLONE_DOWNLOAD_TIMEOUT_MS,
      });
      tempFilePath = downloadResult.tempFilePath;
      downloadedBytes = downloadResult.downloadedBytes;
      mimeType = downloadResult.mimeType;
    } finally {
      stopProgressLog();
    }

    const validateResult = await validateDownloadedFile({
      filePath: tempFilePath,
      expectedSize,
      enableFfprobe: CLONE_DOWNLOAD_VALIDATE_FFPROBE,
    });

    if (!validateResult.ok) {
      logger.warn('[clone][下载/Download] 下载文件校验失败 / downloaded file validation failed', {
        taskId: job.taskId,
        runId: job.runId,
        itemId: job.itemId,
        tempFilePath,
        reason: validateResult.reason,
      });
      await rm(tempFilePath, { force: true });
      throw new Error(`download_validation_failed: ${validateResult.reason}`);
    }

    logger.info('[clone][下载/Download] 下载文件校验通过 / downloaded file validation passed', {
      taskId: job.taskId,
      runId: job.runId,
      itemId: job.itemId,
      tempFilePath,
    });

    const { finalPath } = await finalizeDownloadedFile({
      tempFilePath,
      finalDir: targetPath,
      finalFileName: fileName,
    });

    await prisma.cloneCrawlItem.update({
      where: { id: itemId },
      data: {
        downloadStatus: 'downloaded',
        downloadErrorCode: null,
        downloadError: null,
        localPath: finalPath,
        mimeType: item.mimeType ?? mimeType,
      },
    });

    await prisma.cloneCrawlRun.updateMany({
      where: { id: runId },
      data: {
        downloadedCount: { increment: 1 },
        diskUsedPercent: guard.diskUsagePercent,
      },
    });

    logger.info('[clone][下载/Download] 下载任务完成 / download job completed', {
      taskId: job.taskId,
      runId: job.runId,
      itemId: job.itemId,
      downloadedBytes: downloadedBytes.toString(),
      finalPath,
      diskUsagePercent: guard.diskUsagePercent,
    });
  } catch (err) {
    const reason = classifyRetryReason(err);

    await cloneRetryQueue.add(
      'clone-download-retry',
      {
        queue: 'download',
        reason,
        retryCount: job.retryCount ?? 0,
        payload: {
          ...job,
          retryCount: (job.retryCount ?? 0) + 1,
        },
        firstFailedAt: new Date().toISOString(),
        lastErrorMessage: err instanceof Error ? err.message : String(err),
      },
      { removeOnComplete: true, removeOnFail: 100 },
    );

    await prisma.cloneCrawlItem.updateMany({
      where: { id: itemId },
      data: {
        downloadStatus: 'failed_retryable',
        downloadErrorCode: reason,
        downloadError: err instanceof Error ? err.message : String(err),
        retryCount: { increment: 1 },
      },
    });

    logger.warn('[clone][下载/Download] 下载失败，已进入重试队列 / download failed, retry queued', {
      taskId: job.taskId,
      runId: job.runId,
      itemId: job.itemId,
      reason,
      queue: cloneRetryQueue.name,
      error: err instanceof Error ? err.message : String(err),
    });
  }
}

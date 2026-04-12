import { access, copyFile, mkdir, open, rename, rm, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { constants as fsConstants } from 'node:fs';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { prisma } from '../../infra/prisma';
import { cloneGuardWaitQueue, cloneRetryQueue } from '../../infra/redis';
import {
  CLONE_DOWNLOAD_CHUNK_SIZE_BYTES,
  CLONE_DOWNLOAD_TIMEOUT_MS,
  CLONE_RETRY_MAX,
} from '../constants/clone-queue.constants';
import {
  CLONE_DOWNLOAD_HEARTBEAT_INTERVAL_MS,
  CLONE_DOWNLOAD_LEASE_MS,
} from '../../config/env';
import { logger } from '../../logger';
import { CloneMediaRef, CloneRetryReason, CloneMediaDownloadJob } from '../types/clone-queue.types';
import { checkDownloadGuards, recordGuardTriggered } from './clone-guard.service';
import { tryAcquireCloneChannelSlot, releaseCloneChannelSlot } from './clone-channel-fairness.service';
import { withClient } from './clone-session.service';

const execFileAsync = promisify(execFile);

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

function resolveFileExtensionByMime(mimeType?: string | null) {
  const normalized = (mimeType ?? '').toLowerCase();
  if (normalized === 'video/mp4') return '.mp4';
  if (normalized === 'video/webm') return '.webm';
  if (normalized === 'video/x-matroska' || normalized === 'video/mkv') return '.mkv';
  if (normalized === 'image/jpeg' || normalized === 'image/jpg') return '.jpg';
  if (normalized === 'image/png') return '.png';
  if (normalized === 'image/webp') return '.webp';
  if (normalized === 'image/gif') return '.gif';
  return '.mp4';
}

function splitVideoBaseAndExt(fileNameRaw: string) {
  const cleaned = sanitizeFileName(fileNameRaw || 'media').replace(/\.+$/g, '');
  const m = cleaned.match(/^(.*?)(\.(mp4|mkv|webm|jpg|jpeg|png|webp|gif))$/i);
  if (m) {
    return {
      stem: (m[1] || 'video').trim() || 'video',
      ext: `.${String(m[3]).toLowerCase()}`,
    };
  }

  return {
    stem: cleaned.trim() || 'media',
    ext: '',
  };
}

function resolveTargetFileName(params: {
  channelUsername: string;
  messageId: bigint;
  expectedFileName?: string;
  mimeType?: string | null;
}) {
  const rawBaseName =
    params.expectedFileName || `${params.channelUsername.replace(/^@/, '')}-${params.messageId.toString()}`;
  const { stem, ext } = splitVideoBaseAndExt(rawBaseName);
  const safeExt = ext || resolveFileExtensionByMime(params.mimeType);
  return `${stem}${safeExt}`;
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
}): Promise<{ finalPath: string; finalStemPath: string }> {
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

  const finalExt = path.extname(candidate);
  const finalStemPath = finalExt ? candidate.slice(0, -finalExt.length) : candidate;

  return { finalPath: candidate, finalStemPath };
}

async function persistMessageTextFile(params: {
  finalStemPath: string;
  messageText?: string | null;
}) {
  const text = (params.messageText ?? '').trim();
  if (!text) return;

  const txtPath = `${params.finalStemPath}.txt`;
  await writeFile(txtPath, text, 'utf8');
}

async function validateDownloadedFile(params: {
  filePath: string;
  expectedSize?: bigint;
  expectedMimeType?: string;
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

    const expectedMime = (params.expectedMimeType ?? '').toLowerCase();
    const isImage = expectedMime.startsWith('image/');

    if (isImage) {
      return { ok: true };
    }

    const { stdout } = await execFileAsync('ffprobe', [
      '-v',
      'error',
      '-print_format',
      'json',
      '-show_streams',
      '-show_format',
      params.filePath,
    ]);

    const parsed = JSON.parse(stdout || '{}') as {
      streams?: Array<{ codec_type?: string }>;
      format?: { duration?: string };
    };

    const hasVideoStream = Array.isArray(parsed.streams)
      ? parsed.streams.some((s) => String(s?.codec_type || '').toLowerCase() === 'video')
      : false;

    const duration = Number(parsed.format?.duration ?? '0');
    if (!hasVideoStream) {
      return { ok: false, reason: 'ffprobe_no_video_stream' };
    }
    if (!Number.isFinite(duration) || duration <= 0) {
      return { ok: false, reason: `ffprobe_invalid_duration:${parsed.format?.duration ?? 'null'}` };
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

function normalizePathForCompare(p: string) {
  const resolved = path.resolve(p);
  return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
}

function isSamePath(a: string, b: string) {
  return normalizePathForCompare(a) === normalizePathForCompare(b);
}

function getResumeTempPath(targetDir: string, fileName: string) {
  return path.resolve(path.join(targetDir, `${sanitizeFileName(fileName)}.part`));
}

async function readHeadHex(filePath: string, maxBytes = 64) {
  const fh = await open(filePath, 'r');
  try {
    const buf = Buffer.alloc(maxBytes);
    const { bytesRead } = await fh.read(buf, 0, maxBytes, 0);
    return buf.subarray(0, bytesRead).toString('hex');
  } finally {
    await fh.close();
  }
}

function classifyByMagicHex(headHex: string) {
  if (!headHex) return 'unknown';
  const h = headHex.toLowerCase();
  if (h.startsWith('1a45dfa3')) return 'matroska_or_webm';
  if (h.includes('66747970')) return 'mp4_or_isobmff';
  if (h.startsWith('47494638')) return 'gif';
  if (h.startsWith('ffd8ff')) return 'jpeg';
  if (h.startsWith('89504e47')) return 'png';
  if (h.startsWith('3c68746d6c') || h.startsWith('68746d6c')) return 'html_or_text';
  return 'unknown';
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
  onProgress?: (data: { downloadedBytes: number; progressPct: number | null; speedMbps: number }) => Promise<void>;
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

      const normalizedProgressPct = progressPct !== null ? Number(progressPct.toFixed(2)) : null;
      const normalizedSpeedMbps = Number(speedMbps.toFixed(2));

      if (params.onProgress) {
        await params.onProgress({
          downloadedBytes: currentSize,
          progressPct: normalizedProgressPct,
          speedMbps: normalizedSpeedMbps,
        });
      }

      logger.info('[clone][下载/Download] 下载进度 / download progress', {
        channelUsername: params.channelUsername,
        messageId: params.messageId,
        downloadedBytes: currentSize,
        expectedBytes: expected > 0 ? expected : null,
        progressPct: normalizedProgressPct,
        speedMbps: normalizedSpeedMbps,
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
  await rm(tempFilePath, { force: true });
  let resolvedMimeType: string | undefined;

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

      const doc = (media as { document?: { mimeType?: string; attributes?: unknown[] }; photo?: unknown }).document;
      const photo = (media as { photo?: unknown }).photo;
      const mimeType = typeof doc?.mimeType === 'string' ? doc.mimeType : undefined;
      const attrs = Array.isArray(doc?.attributes) ? doc.attributes : [];
      const hasVideoAttr = attrs.some((attr) => String((attr as { className?: string })?.className ?? '').toLowerCase().includes('video'));
      const isVideo = Boolean((mimeType ?? '').toLowerCase().startsWith('video/') || hasVideoAttr);
      const isImage = Boolean((mimeType ?? '').toLowerCase().startsWith('image/') || photo);
      const supportedMedia = isVideo || isImage;

      if (!supportedMedia) {
        throw new Error(`unsupported_media_type: mime=${mimeType ?? 'unknown'} tg://${channelUsername}/${messageId}`);
      }

      resolvedMimeType = mimeType ?? (isImage ? 'image/jpeg' : undefined);

      const result = await c.downloadMedia(media, {
        outputFile: tempFilePath,
        workers: 1,
        partSizeKb: Math.max(32, Math.floor(CLONE_DOWNLOAD_CHUNK_SIZE_BYTES / 1024)),
      });

      if (typeof result === 'string') {
        const resultPath = path.resolve(result);
        if (!isSamePath(resultPath, tempFilePath)) {
          const s = await stat(resultPath);
          if (!s.isFile() || s.size <= 0) {
            throw new Error(`download_result_path_invalid: ${resultPath}`);
          }
          await copyFile(resultPath, tempFilePath);
        }
      } else if (result instanceof Uint8Array || Buffer.isBuffer(result)) {
        await writeFile(tempFilePath, Buffer.from(result));
      }

      logger.info('[clone][下载/Download] Telegram 下载调用返回 / telegram downloadMedia returned', {
        channelUsername,
        messageId,
        resultType:
          typeof result === 'string'
            ? 'path'
            : result instanceof Uint8Array || Buffer.isBuffer(result)
              ? 'bytes'
              : String(result),
        resultPath: typeof result === 'string' ? path.resolve(result) : null,
        resultEqualsTempPath: typeof result === 'string' ? isSamePath(path.resolve(result), tempFilePath) : null,
        mimeType: mimeType ?? null,
        isVideo,
        isImage,
      });

      return true;
    });

    if (!downloaded) {
      throw new Error(`download_unknown_error: unable to download tg://${channelUsername}/${messageId}`);
    }
  } else {
    throw new Error(`unsupported_media_ref: kind=${params.mediaRef.kind}`);
  }

  const fsStat = await stat(tempFilePath);
  return {
    tempFilePath,
    downloadedBytes: BigInt(Math.max(0, fsStat.size)),
    mimeType: resolvedMimeType,
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

export async function processCloneMediaDownload(job: CloneMediaDownloadJob, workerJobId?: string) {
  const itemId = BigInt(job.itemId);
  const runId = BigInt(job.runId);
  const taskId = BigInt(job.taskId);

  const channelUsernameForSlot = (job.channelUsername ?? '').trim().replace(/^@+/, '').toLowerCase();
  const channelSlot = channelUsernameForSlot
    ? await tryAcquireCloneChannelSlot(channelUsernameForSlot)
    : null;

  if (channelSlot?.staleReplaced) {
    logger.warn('[clone][下载/Download] 发现并替换 stale channel slot / stale channel slot replaced', {
      taskId: job.taskId,
      runId: job.runId,
      itemId: job.itemId,
      channelUsername: channelUsernameForSlot,
      slotKey: channelSlot.key,
    });
  }

  if (channelSlot && !channelSlot.acquired) {
    await cloneGuardWaitQueue.add(
      'clone-download-channel-slot-wait',
      {
        ...job,
        retryCount: job.retryCount ?? 0,
      },
      {
        delay: 10_000,
        removeOnComplete: true,
        removeOnFail: 100,
      },
    );

    await recordGuardTriggered({
      itemId,
      reason: 'per_channel_concurrency_exceeded',
      detail: 'channel_slot_busy, retry after 10000ms',
    });

    logger.warn('[clone][下载/Download] 频道槽位占用，已排队等待 / channel slot busy, delayed', {
      taskId: job.taskId,
      runId: job.runId,
      itemId: job.itemId,
      channelUsername: channelUsernameForSlot,
      delayMs: 10_000,
    });
    return;
  }

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
      await cloneGuardWaitQueue.add(
        'clone-download-guard-wait',
        {
          ...job,
          retryCount: job.retryCount ?? 0,
        },
        {
          delay: guard.retryDelayMs,
          removeOnComplete: true,
          removeOnFail: 100,
        },
      );
      return;
    }

    await prisma.cloneCrawlItem.update({
      where: { id: itemId },
      data: {
        downloadStatus: 'downloading',
        downloadLeaseUntil: new Date(Date.now() + CLONE_DOWNLOAD_LEASE_MS),
        downloadHeartbeatAt: new Date(),
        downloadWorkerJobId: workerJobId ?? null,
        downloadAttempt: { increment: 1 },
        downloadProgressPct: 0,
        downloadedBytes: BigInt(0),
        downloadSpeedMbps: null,
        downloadErrorCode: null,
        downloadError: null,
      } as any,
    });

    const expectedSize = toSafeBigInt(job.expectedFileSize) ?? item.fileSize ?? undefined;
    const mediaRef = parseMediaRef(job.mediaRef, {
      channelUsername: item.channelUsername,
      messageId: item.messageId,
    });
    const resolvedMimeHint = job.expectedMimeType ?? item.mimeType;
    const fileName = resolveTargetFileName({
      channelUsername: item.channelUsername,
      messageId: item.messageId,
      expectedFileName: job.expectedFileName,
      mimeType: resolvedMimeHint,
    });

    logger.info('[clone][下载/Download] 下载参数已解析 / download params resolved', {
      taskId: job.taskId,
      runId: job.runId,
      itemId: job.itemId,
      mediaRefKind: mediaRef.kind,
      targetPath,
      expectedSize: expectedSize?.toString() ?? null,
      expectedMimeType: job.expectedMimeType ?? item.mimeType ?? null,
      fileName,
    });

    const currentTempPath = getResumeTempPath(targetPath, fileName);

    logger.info('[clone][下载/Download] 下载前状态 / pre-download state', {
      taskId: job.taskId,
      runId: job.runId,
      itemId: job.itemId,
      tempFilePath: currentTempPath,
      expectedSize: expectedSize?.toString() ?? null,
      expectedMimeType: job.expectedMimeType ?? item.mimeType ?? null,
      retryCount: job.retryCount ?? 0,
    });

    const stopProgressLog = startProgressLogger({
      tempFilePath: currentTempPath,
      channelUsername: mediaRef.kind === 'tg_message' ? mediaRef.channelUsername : item.channelUsername,
      messageId: mediaRef.kind === 'tg_message' ? Number(mediaRef.messageId) : Number(item.messageId),
      expectedSize,
      onProgress: async ({ downloadedBytes, progressPct, speedMbps }) => {
        await prisma.cloneCrawlItem.updateMany({
          where: { id: itemId, downloadStatus: 'downloading' },
          data: {
            downloadedBytes: BigInt(Math.max(0, downloadedBytes)),
            downloadProgressPct: progressPct === null ? 0 : Math.max(0, Math.min(100, Math.floor(progressPct))),
            downloadSpeedMbps: speedMbps,
            downloadHeartbeatAt: new Date(),
            downloadLeaseUntil: new Date(Date.now() + CLONE_DOWNLOAD_LEASE_MS),
            downloadWorkerJobId: workerJobId ?? null,
          } as any,
        });
      },
    });

    const heartbeatTimer = setInterval(() => {
      void prisma.cloneCrawlItem.updateMany({
        where: { id: itemId, downloadStatus: 'downloading' },
        data: {
          downloadLeaseUntil: new Date(Date.now() + CLONE_DOWNLOAD_LEASE_MS),
          downloadHeartbeatAt: new Date(),
          downloadWorkerJobId: workerJobId ?? null,
        } as any,
      });
    }, CLONE_DOWNLOAD_HEARTBEAT_INTERVAL_MS);

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
      clearInterval(heartbeatTimer);
      stopProgressLog();
    }

    const tempStat = await stat(tempFilePath);
    const headHex = await readHeadHex(tempFilePath, 64);
    const magicGuess = classifyByMagicHex(headHex);

    const expectedSizeNumber = expectedSize ? Number(expectedSize) : null;
    const sizeDelta = expectedSizeNumber !== null ? tempStat.size - expectedSizeNumber : null;

    logger.info('[clone][下载/Download] 下载后文件快照 / post-download file snapshot', {
      taskId: job.taskId,
      runId: job.runId,
      itemId: job.itemId,
      tempFilePath,
      actualSize: tempStat.size,
      expectedSize: expectedSizeNumber,
      sizeDelta,
      magicGuess,
      headHex,
      mimeTypeResolved: mimeType ?? null,
      mimeTypeExpected: job.expectedMimeType ?? item.mimeType ?? null,
    });

    if (expectedSizeNumber !== null && tempStat.size + 1024 < expectedSizeNumber) {
      throw new Error(`download_size_incomplete: expected=${expectedSizeNumber}, actual=${tempStat.size}`);
    }

    const validateResult = await validateDownloadedFile({
      filePath: tempFilePath,
      expectedSize,
      expectedMimeType: job.expectedMimeType ?? item.mimeType ?? mimeType,
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

    const { finalPath, finalStemPath } = await finalizeDownloadedFile({
      tempFilePath,
      finalDir: targetPath,
      finalFileName: fileName,
    });

    await persistMessageTextFile({
      finalStemPath,
      messageText: item.messageText,
    });

    await prisma.cloneCrawlItem.update({
      where: { id: itemId },
      data: {
        downloadStatus: 'downloaded',
        downloadLeaseUntil: null,
        downloadHeartbeatAt: null,
        downloadWorkerJobId: null,
        downloadProgressPct: 100,
        downloadedBytes,
        downloadSpeedMbps: null,
        downloadErrorCode: null,
        downloadError: null,
        localPath: finalPath,
        mimeType: mimeType ?? item.mimeType,
      } as any,
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

    const currentRetry = job.retryCount ?? 0;

    const taskConfig = await prisma.cloneCrawlTask.findUnique({
      where: { id: taskId },
      select: { retryMax: true },
    });

    const retryMax =
      taskConfig?.retryMax && taskConfig.retryMax >= 0
        ? taskConfig.retryMax
        : CLONE_RETRY_MAX;

    if (currentRetry >= retryMax) {
      await prisma.cloneCrawlItem.updateMany({
        where: { id: itemId },
        data: {
          downloadStatus: 'failed_final',
          downloadLeaseUntil: null,
          downloadHeartbeatAt: null,
          downloadWorkerJobId: null,
          downloadErrorCode: 'retry_exhausted',
          downloadError: err instanceof Error ? err.message : String(err),
        } as any,
      });

      logger.warn('[clone][下载/Download] 下载失败已达重试上限 / download failed and reached retry max', {
        taskId: job.taskId,
        runId: job.runId,
        itemId: job.itemId,
        reason,
        retryCountCurrent: currentRetry,
        retryMax,
        error: err instanceof Error ? err.message : String(err),
      });
      return;
    }

    await cloneRetryQueue.add(
      'clone-download-retry',
      {
        queue: 'download',
        reason,
        retryCount: currentRetry,
        payload: {
          ...job,
          retryCount: currentRetry + 1,
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
        downloadLeaseUntil: null,
        downloadHeartbeatAt: null,
        downloadWorkerJobId: null,
        downloadErrorCode: reason,
        downloadError: err instanceof Error ? err.message : String(err),
        retryCount: { increment: 1 },
      } as any,
    });

    logger.warn('[clone][下载/Download] 下载失败，已进入重试队列 / download failed, retry queued', {
      taskId: job.taskId,
      runId: job.runId,
      itemId: job.itemId,
      reason,
      queue: cloneRetryQueue.name,
      retryCountCurrent: job.retryCount ?? 0,
      retryCountNext: (job.retryCount ?? 0) + 1,
      expectedMimeType: job.expectedMimeType ?? null,
      expectedFileSize: job.expectedFileSize ?? null,
      mediaRefKind: job.mediaRef?.kind ?? null,
      error: err instanceof Error ? err.message : String(err),
    });
  } finally {
    if (channelSlot?.acquired) {
      await releaseCloneChannelSlot({
        key: channelSlot.key,
        token: channelSlot.token,
      });
    }
  }
}

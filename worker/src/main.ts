import { Queue, Worker } from 'bullmq';
import IORedis from 'ioredis';
import dotenv from 'dotenv';
import { createReadStream, openAsBlob } from 'node:fs';
import { mkdir, readdir, rename, stat } from 'node:fs/promises';
import { basename, dirname, extname, join, resolve } from 'node:path';
import { createHash } from 'node:crypto';
import {
  CatalogTaskStatus,
  MediaStatus,
  PrismaClient,
  TaskDefinitionType,
  TaskStatus,
} from '@prisma/client';

dotenv.config({ path: '../.env' });
dotenv.config();

const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
const telegramApiBase = process.env.TELEGRAM_BOT_API_BASE || 'https://api.telegram.org';

const connection = new IORedis(redisUrl, { maxRetriesPerRequest: null });
const prisma = new PrismaClient();

const dispatchQueue = new Queue('q_dispatch', { connection });
const catalogQueue = new Queue('q_catalog', { connection });
const relayUploadQueue = new Queue('q_relay_upload', { connection });

const SCHEDULER_POLL_MS = 5000;
const MAX_SCHEDULE_BATCH = 100;
const RELAY_MIN_STABLE_CHECKS = Number(process.env.RELAY_MIN_STABLE_CHECKS || '3');
const RELAY_STABLE_INTERVAL_MS = Number(process.env.RELAY_STABLE_INTERVAL_MS || '10000');
const RELAY_MTIME_COOLDOWN_MS = Number(process.env.RELAY_MTIME_COOLDOWN_MS || '60000');

const SUPPORTED_VIDEO_EXT = new Set([
  '.mp4',
  '.mkv',
  '.mov',
  '.avi',
  '.m4v',
  '.webm',
]);

let hasWarnedMissingTaskDefinitionsTable = false;

function getTaskDefinitionModel() {
  const model = prisma.taskDefinition;
  if (!model) {
    throw new Error(
      'Prisma taskDefinition model is unavailable. Please run prisma generate and restart worker.',
    );
  }

  return model;
}

function getBackoffSeconds(retryCount: number): number {
  const base = Math.max(1, Math.pow(2, retryCount));
  return Math.min(base * 30, 3600);
}

function normalizeTelegramApiBase(raw: string): string {
  return raw.replace(/\/+$/, '');
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function moveToArchive(localPath: string): Promise<string> {
  // Skip if already in an archived folder
  if (localPath.includes(`${join('archived')}`)) {
    return localPath;
  }

  const fileName = basename(localPath);
  // localPath: .../data/tg-crm/channels/1003896883365/file.mp4
  const channelIdDir = dirname(localPath);              // .../channels/1003896883365
  const channelDirName = basename(channelIdDir);        // 1003896883365
  const channelsDir = dirname(channelIdDir);            // .../channels
  const dataBaseDir = dirname(channelsDir);             // .../data/tg-crm
  const archiveDir = join(dataBaseDir, 'archived', channelDirName);
  await mkdir(archiveDir, { recursive: true });
  const archivePath = join(archiveDir, fileName);
  await rename(localPath, archivePath);
  // eslint-disable-next-line no-console
  console.log(`[archive] moved ${localPath} -> ${archivePath}`);
  return archivePath;
}

async function waitForFileStable(filePath: string) {
  let previousSize: bigint | null = null;
  let stableCount = 0;

  while (stableCount < RELAY_MIN_STABLE_CHECKS) {
    const s = await stat(filePath);

    if (previousSize !== null && s.size === previousSize) {
      stableCount += 1;
    } else {
      stableCount = 0;
      previousSize = s.size;
    }

    if (stableCount < RELAY_MIN_STABLE_CHECKS) {
      await sleep(RELAY_STABLE_INTERVAL_MS);
    }
  }

  const finalStat = await stat(filePath);
  const ageMs = Date.now() - finalStat.mtimeMs;
  if (ageMs < RELAY_MTIME_COOLDOWN_MS) {
    await sleep(RELAY_MTIME_COOLDOWN_MS - ageMs);
  }
}

function toTelegramMessageLink(
  chatIdRaw: string,
  messageId: number,
): string | null {
  if (chatIdRaw.startsWith('-100')) {
    const internalId = chatIdRaw.slice(4);
    return `https://t.me/c/${internalId}/${messageId}`;
  }

  if (chatIdRaw.startsWith('@')) {
    return `https://t.me/${chatIdRaw.slice(1)}/${messageId}`;
  }

  return null;
}

type TelegramSendResult = {
  messageId: number;
  messageLink: string | null;
};

type TelegramError = {
  code: string;
  message: string;
  retryAfterSec?: number;
};

async function sendTelegramRequest(args: {
  botToken: string;
  method: 'sendVideo' | 'sendDocument' | 'sendMessage' | 'pinChatMessage';
  payload: Record<string, unknown> | FormData;
}): Promise<{ messageId?: number; videoFileId?: string; videoFileUniqueId?: string }> {
  const endpoint = `${normalizeTelegramApiBase(telegramApiBase)}/bot${args.botToken}/${args.method}`;

  const isFormData = args.payload instanceof FormData;
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: isFormData
      ? undefined
      : {
        'content-type': 'application/json',
      },
    body: isFormData ? args.payload : JSON.stringify(args.payload),
  });

  const json = (await response.json()) as {
    ok: boolean;
    result?: {
      message_id?: number;
      video?: { file_id?: string; file_unique_id?: string };
      document?: { file_id?: string; file_unique_id?: string };
    } | true;
    error_code?: number;
    description?: string;
    parameters?: { retry_after?: number };
  };

  if (!response.ok || !json.ok) {
    const errorCode = json.error_code ?? response.status;
    const description = json.description || `Telegram API HTTP ${response.status}`;

    const err: TelegramError = {
      code: `TG_${errorCode}`,
      message: description,
      retryAfterSec: json.parameters?.retry_after,
    };

    throw err;
  }

  return {
    messageId:
      json.result && typeof json.result === 'object'
        ? json.result.message_id
        : undefined,
    videoFileId:
      json.result && typeof json.result === 'object'
        ? (json.result.video?.file_id ?? json.result.document?.file_id)
        : undefined,
    videoFileUniqueId:
      json.result && typeof json.result === 'object'
        ? (json.result.video?.file_unique_id ?? json.result.document?.file_unique_id)
        : undefined,
  };
}

async function sendVideoByTelegram(args: {
  botToken: string;
  chatId: string;
  fileId: string;
  caption?: string | null;
  parseMode?: string | null;
  replyMarkup?: unknown;
}): Promise<TelegramSendResult> {
  const payload: Record<string, unknown> = {
    chat_id: args.chatId,
    video: args.fileId,
  };

  if (args.caption) payload.caption = args.caption;
  if (args.parseMode) payload.parse_mode = args.parseMode;
  if (args.replyMarkup) payload.reply_markup = args.replyMarkup;

  const result = await sendTelegramRequest({
    botToken: args.botToken,
    method: 'sendVideo',
    payload,
  });

  return {
    messageId: result.messageId,
    messageLink: toTelegramMessageLink(args.chatId, result.messageId),
  };
}

async function sendTextByTelegram(args: {
  botToken: string;
  chatId: string;
  text: string;
  parseMode?: string;
}): Promise<TelegramSendResult> {
  const result = await sendTelegramRequest({
    botToken: args.botToken,
    method: 'sendMessage',
    payload: {
      chat_id: args.chatId,
      text: args.text,
      parse_mode: args.parseMode ?? 'HTML',
      disable_web_page_preview: true,
    },
  });

  if (!result.messageId) {
    throw new Error('sendMessage response missing message_id');
  }

  return {
    messageId: result.messageId,
    messageLink: toTelegramMessageLink(args.chatId, result.messageId),
  };
}

async function pinMessageByTelegram(args: {
  botToken: string;
  chatId: string;
  messageId: number;
}) {
  await sendTelegramRequest({
    botToken: args.botToken,
    method: 'pinChatMessage',
    payload: {
      chat_id: args.chatId,
      message_id: args.messageId,
      disable_notification: true,
    },
  });
}

async function updateTaskDefinitionRunStatus(args: {
  taskDefinitionId: bigint;
  status: 'success' | 'failed';
  summary: Record<string, unknown>;
}) {
  await getTaskDefinitionModel().update({
    where: { id: args.taskDefinitionId },
    data: {
      lastRunAt: new Date(),
      lastRunStatus: args.status,
      lastRunSummary: args.summary,
    },
  });
}

async function hashFile(filePath: string): Promise<string> {
  return new Promise((resolveHash, reject) => {
    const hash = createHash('sha256');
    const stream = createReadStream(filePath);

    stream.on('data', (chunk) => hash.update(chunk));
    stream.on('end', () => resolveHash(hash.digest('hex')));
    stream.on('error', reject);
  });
}

async function scanChannelVideos(folderPath: string) {
  const absolute = resolve(folderPath);
  const entries = await readdir(absolute, { withFileTypes: true });

  const files = entries
    .filter((entry) => entry.isFile())
    .map((entry) => resolve(absolute, entry.name))
    .filter((filePath) => SUPPORTED_VIDEO_EXT.has(extname(filePath).toLowerCase()));

  return files;
}

async function enqueueRelayAssetsFromTaskDefinition(taskDefinitionId: bigint) {
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

      const s = await stat(filePath);
      const fileHash = await hashFile(filePath);

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
            },
          },
          select: { id: true, status: true },
        });
        createdAssets += 1;
      }

      if (
        asset.status === MediaStatus.relay_uploaded ||
        asset.status === MediaStatus.ingesting
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

async function scheduleDispatchForDefinition(taskDefinitionId: bigint) {
  try {
    await scheduleDueDispatchTasks();
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'success',
      summary: {
        executor: 'dispatch_send',
        message: 'dispatch scheduler tick completed',
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'unknown error';
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'failed',
      summary: {
        executor: 'dispatch_send',
        error: message,
      },
    });

    throw error;
  }
}

async function scheduleRelayForDefinition(taskDefinitionId: bigint) {
  try {
    const enqueueSummary = await enqueueRelayAssetsFromTaskDefinition(taskDefinitionId);
    await scheduleDueRelayUploadTasks();
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'success',
      summary: {
        executor: 'relay_upload',
        ...enqueueSummary,
        message: 'relay upload scan + scheduler tick completed',
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'unknown error';
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'failed',
      summary: {
        executor: 'relay_upload',
        error: message,
      },
    });

    throw error;
  }
}

async function scheduleCatalogForDefinition(taskDefinitionId: bigint) {
  try {
    await scheduleDueCatalogTasks();
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'success',
      summary: {
        executor: 'catalog_publish',
        message: 'catalog scheduler tick completed',
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'unknown error';
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'failed',
      summary: {
        executor: 'catalog_publish',
        error: message,
      },
    });

    throw error;
  }
}

async function scheduleEnabledTaskDefinitions() {
  let definitions: Array<{ id: bigint; taskType: TaskDefinitionType }> = [];

  try {
    definitions = await getTaskDefinitionModel().findMany({
      where: { isEnabled: true },
      orderBy: [{ priority: 'asc' }, { updatedAt: 'asc' }],
      take: MAX_SCHEDULE_BATCH,
      select: {
        id: true,
        taskType: true,
      },
    });
  } catch (error) {
    const isTableMissing =
      typeof error === 'object' &&
      error !== null &&
      'code' in error &&
      (error as { code?: string }).code === 'P2021';

    if (isTableMissing) {
      if (!hasWarnedMissingTaskDefinitionsTable) {
        // eslint-disable-next-line no-console
        console.warn(
          '[scheduler:task-definitions] table task_definitions not found, fallback to legacy schedulers. Run prisma migrate to enable task definitions.',
        );
        hasWarnedMissingTaskDefinitionsTable = true;
      }

      await scheduleDueDispatchTasks();
      await scheduleDueRelayUploadTasks();
      await scheduleDueCatalogTasks();
      return;
    }

    throw error;
  }

  for (const definition of definitions) {
    if (definition.taskType === TaskDefinitionType.relay_upload) {
      await scheduleRelayForDefinition(definition.id);
      continue;
    }

    if (definition.taskType === TaskDefinitionType.dispatch_send) {
      await scheduleDispatchForDefinition(definition.id);
      continue;
    }

    if (definition.taskType === TaskDefinitionType.catalog_publish) {
      await scheduleCatalogForDefinition(definition.id);
    }
  }
}

async function scheduleDueDispatchTasks() {
  const now = new Date();

  const dueTasks = await prisma.dispatchTask.findMany({
    where: {
      status: { in: [TaskStatus.pending, TaskStatus.scheduled, TaskStatus.failed] },
      nextRunAt: { lte: now },
    },
    orderBy: [{ priority: 'asc' }, { nextRunAt: 'asc' }],
    take: MAX_SCHEDULE_BATCH,
    select: {
      id: true,
      status: true,
      channelId: true,
      mediaAssetId: true,
      retryCount: true,
    },
  });

  for (const task of dueTasks) {
    const updated = await prisma.dispatchTask.updateMany({
      where: {
        id: task.id,
        status: {
          in: [TaskStatus.pending, TaskStatus.scheduled, TaskStatus.failed],
        },
      },
      data: {
        status: TaskStatus.scheduled,
      },
    });

    if (updated.count === 0) continue;

    await dispatchQueue.add(
      'dispatch-send',
      {
        dispatchTaskId: task.id.toString(),
        channelId: task.channelId.toString(),
        mediaAssetId: task.mediaAssetId.toString(),
        retryCount: task.retryCount,
      },
      {
        jobId: `dispatch-${task.id.toString()}`,
        removeOnComplete: true,
        removeOnFail: 200,
      },
    );
  }

  if (dueTasks.length > 0) {
    // eslint-disable-next-line no-console
    console.log(`[scheduler] queued ${dueTasks.length} dispatch task(s)`);
  }
}

async function scheduleDueRelayUploadTasks() {
  const staleIngestingBefore = new Date(Date.now() - 5 * 60 * 1000);

  const dueAssets = await prisma.mediaAsset.findMany({
    where: {
      telegramFileId: null,
      OR: [
        { status: MediaStatus.ready },
        {
          status: MediaStatus.ingesting,
          updatedAt: { lte: staleIngestingBefore },
        },
      ],
    },
    orderBy: { createdAt: 'asc' },
    take: MAX_SCHEDULE_BATCH,
    select: {
      id: true,
      channelId: true,
      status: true,
      sourceMeta: true,
      updatedAt: true,
    },
  });

  let queuedCount = 0;

  for (const asset of dueAssets) {
    const sourceMeta = (asset.sourceMeta ?? {}) as Record<string, unknown>;
    const relayChannelId = sourceMeta.relayChannelId;
    if (typeof relayChannelId !== 'string' || !relayChannelId.trim()) continue;

    const whereReady = {
      id: asset.id,
      status: MediaStatus.ready,
      telegramFileId: null,
    };

    const whereStaleIngesting = {
      id: asset.id,
      status: MediaStatus.ingesting,
      telegramFileId: null,
      updatedAt: { lte: staleIngestingBefore },
    };

    const updated = await prisma.mediaAsset.updateMany({
      where: asset.status === MediaStatus.ready ? whereReady : whereStaleIngesting,
      data: {
        status: MediaStatus.ingesting,
        updatedAt: new Date(),
      },
    });

    if (updated.count === 0) continue;

    const jobId = `relay-upload-${asset.id.toString()}`;
    const existingJob = await relayUploadQueue.getJob(jobId);
    if (existingJob) {
      const state = await existingJob.getState();
      if (state === 'failed') {
        await existingJob.remove();
      } else {
        continue;
      }
    }

    await relayUploadQueue.add(
      'relay-upload',
      {
        mediaAssetId: asset.id.toString(),
        relayChannelId,
      },
      {
        jobId,
        removeOnComplete: true,
        removeOnFail: 200,
      },
    );

    queuedCount += 1;
  }

  if (queuedCount > 0) {
    // eslint-disable-next-line no-console
    console.log(`[scheduler] queued ${queuedCount} relay upload task(s)`);
  }
}

async function scheduleDueCatalogTasks() {
  const now = new Date();

  const dueTasks = await prisma.catalogTask.findMany({
    where: {
      status: CatalogTaskStatus.pending,
      OR: [{ plannedAt: null }, { plannedAt: { lte: now } }],
    },
    orderBy: [{ plannedAt: 'asc' }, { createdAt: 'asc' }],
    take: MAX_SCHEDULE_BATCH,
    select: {
      id: true,
      channelId: true,
      catalogTemplateId: true,
    },
  });

  for (const task of dueTasks) {
    const updated = await prisma.catalogTask.updateMany({
      where: {
        id: task.id,
        status: CatalogTaskStatus.pending,
      },
      data: {
        status: CatalogTaskStatus.running,
        startedAt: new Date(),
      },
    });

    if (updated.count === 0) continue;

    await catalogQueue.add(
      'catalog-publish',
      {
        catalogTaskId: task.id.toString(),
        channelId: task.channelId.toString(),
      },
      {
        jobId: `catalog-${task.id.toString()}`,
        removeOnComplete: true,
        removeOnFail: 200,
      },
    );
  }

  if (dueTasks.length > 0) {
    // eslint-disable-next-line no-console
    console.log(`[scheduler] queued ${dueTasks.length} catalog task(s)`);
  }
}

const dispatchWorker = new Worker(
  'q_dispatch',
  async (job) => {
    if (job.name === 'bootstrap-check') {
      return { ok: true, skipped: true, reason: 'bootstrap-check' };
    }

    const dispatchTaskIdRaw = job.data.dispatchTaskId as string | undefined;
    if (!dispatchTaskIdRaw) {
      throw new Error('Missing dispatchTaskId in job payload');
    }

    const dispatchTaskId = BigInt(dispatchTaskIdRaw);

    const task = await prisma.dispatchTask.findUnique({
      where: { id: dispatchTaskId },
      include: {
        channel: {
          select: {
            id: true,
            tgChatId: true,
            defaultBotId: true,
          },
        },
        mediaAsset: {
          select: {
            id: true,
            telegramFileId: true,
            status: true,
          },
        },
      },
    });

    if (!task) {
      throw new Error(`Dispatch task not found: ${dispatchTaskIdRaw}`);
    }

    await prisma.dispatchTask.update({
      where: { id: dispatchTaskId },
      data: {
        status: TaskStatus.running,
        startedAt: new Date(),
      },
    });

    await prisma.dispatchTaskLog.create({
      data: {
        dispatchTaskId,
        action: 'task_running',
        detail: {
          jobId: String(job.id),
          attemptsMade: job.attemptsMade,
        },
      },
    });

    try {
      if (!task.mediaAsset.telegramFileId) {
        throw new Error('Media asset has no telegramFileId (relay not uploaded)');
      }

      const resolvedBotId = task.botId ?? task.channel.defaultBotId;
      if (!resolvedBotId) {
        throw new Error('No bot assigned to dispatch task or channel');
      }

      const bot = await prisma.bot.findUnique({
        where: { id: resolvedBotId },
        select: {
          id: true,
          status: true,
          tokenEncrypted: true,
        },
      });

      if (!bot) {
        throw new Error(`Bot not found: ${resolvedBotId.toString()}`);
      }

      if (bot.status !== 'active') {
        throw new Error(`Bot is not active: ${bot.status}`);
      }

      const sendResult = await sendVideoByTelegram({
        botToken: bot.tokenEncrypted,
        chatId: task.channel.tgChatId,
        fileId: task.mediaAsset.telegramFileId,
        caption: task.caption,
        parseMode: task.parseMode,
        replyMarkup: task.replyMarkup,
      });

      await prisma.dispatchTask.update({
        where: { id: dispatchTaskId },
        data: {
          status: TaskStatus.success,
          finishedAt: new Date(),
          botId: resolvedBotId,
          telegramMessageId: BigInt(sendResult.messageId),
          telegramMessageLink: sendResult.messageLink,
          telegramErrorCode: null,
          telegramErrorMessage: null,
        },
      });

      await prisma.dispatchTaskLog.create({
        data: {
          dispatchTaskId,
          action: 'task_success',
          detail: {
            botId: resolvedBotId.toString(),
            messageId: sendResult.messageId,
            messageLink: sendResult.messageLink,
          },
        },
      });

      return {
        ok: true,
        dispatchTaskId: dispatchTaskIdRaw,
        messageId: sendResult.messageId,
      };
    } catch (error) {
      const nextRetryCount = task.retryCount + 1;
      const now = new Date();

      const errorObj = error as TelegramError;
      const message = errorObj.message || 'Unknown dispatch error';
      const code = errorObj.code || 'DISPATCH_ERROR';

      const retryAfterSec = errorObj.retryAfterSec;
      const fallbackBackoffSec = getBackoffSeconds(nextRetryCount);
      const finalBackoffSec = retryAfterSec ?? fallbackBackoffSec;
      const nextRunAt = new Date(Date.now() + finalBackoffSec * 1000);

      const exceeded = nextRetryCount > task.maxRetries;

      const nextStatus = exceeded ? TaskStatus.dead : TaskStatus.failed;

      await prisma.dispatchTask.update({
        where: { id: dispatchTaskId },
        data: {
          status: nextStatus,
          retryCount: nextRetryCount,
          nextRunAt: exceeded ? task.nextRunAt : nextRunAt,
          telegramErrorCode: code,
          telegramErrorMessage: message,
          finishedAt: now,
        },
      });

      await prisma.dispatchTaskLog.create({
        data: {
          dispatchTaskId,
          action: nextStatus === TaskStatus.dead ? 'task_dead' : 'task_failed',
          detail: {
            errorCode: code,
            errorMessage: message,
            retryCount: nextRetryCount,
            nextRunAt: exceeded ? null : nextRunAt,
          },
        },
      });

      if (code === 'TG_429' || code === 'TG_403') {
        await prisma.riskEvent.create({
          data: {
            level: code === 'TG_429' ? 'high' : 'critical',
            eventType:
              code === 'TG_429'
                ? 'telegram_rate_limit'
                : 'telegram_permission_denied',
            botId: task.botId ?? task.channel.defaultBotId,
            channelId: task.channelId,
            dispatchTaskId: task.id,
            payload: {
              telegramErrorCode: code,
              telegramErrorMessage: message,
              retryCount: nextRetryCount,
              jobId: String(job.id),
            },
          },
        });
      }

      throw error;
    }
  },
  { connection, concurrency: 5 },
);

const relayUploadWorker = new Worker(
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

    // eslint-disable-next-line no-console
    console.log(
      `[q_relay_upload] uploading mediaAsset=${mediaAssetIdRaw} to relayChannel=${relayChannelIdRaw}`,
    );

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
      // eslint-disable-next-line no-console
      console.warn(`[q_relay_upload] failed to move file to archive:`, moveErr);
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
  { connection, concurrency: 2 },
);

const catalogWorker = new Worker(
  'q_catalog',
  async (job) => {
    if (job.name === 'bootstrap-check') {
      return { ok: true, skipped: true, reason: 'bootstrap-check' };
    }

    const catalogTaskIdRaw = job.data.catalogTaskId as string | undefined;
    if (!catalogTaskIdRaw) {
      throw new Error('Missing catalogTaskId in job payload');
    }

    const catalogTaskId = BigInt(catalogTaskIdRaw);

    const task = await prisma.catalogTask.findUnique({
      where: { id: catalogTaskId },
      include: {
        channel: {
          select: {
            id: true,
            tgChatId: true,
            defaultBotId: true,
          },
        },
      },
    });

    if (!task) {
      throw new Error(`Catalog task not found: ${catalogTaskIdRaw}`);
    }

    try {
      if (!task.contentPreview) {
        throw new Error('Catalog contentPreview is empty');
      }

      const resolvedBotId = task.channel.defaultBotId;
      if (!resolvedBotId) {
        throw new Error('No default bot assigned to channel for catalog publish');
      }

      const bot = await prisma.bot.findUnique({
        where: { id: resolvedBotId },
        select: {
          id: true,
          status: true,
          tokenEncrypted: true,
        },
      });

      if (!bot) {
        throw new Error(`Bot not found: ${resolvedBotId.toString()}`);
      }

      if (bot.status !== 'active') {
        throw new Error(`Bot is not active: ${bot.status}`);
      }

      const sendResult = await sendTextByTelegram({
        botToken: bot.tokenEncrypted,
        chatId: task.channel.tgChatId,
        text: task.contentPreview,
      });

      let pinSuccess: boolean | null = null;
      let pinErrorMessage: string | null = null;

      if (task.pinAfterPublish) {
        try {
          await pinMessageByTelegram({
            botToken: bot.tokenEncrypted,
            chatId: task.channel.tgChatId,
            messageId: sendResult.messageId,
          });
          pinSuccess = true;
        } catch (pinError) {
          pinSuccess = false;
          pinErrorMessage =
            pinError instanceof Error
              ? pinError.message
              : 'Unknown pinChatMessage error';
        }
      }

      await prisma.catalogTask.update({
        where: { id: catalogTaskId },
        data: {
          status: CatalogTaskStatus.success,
          finishedAt: new Date(),
          telegramMessageId: BigInt(sendResult.messageId),
          telegramMessageLink: sendResult.messageLink,
          pinSuccess,
          pinErrorMessage,
          errorMessage: null,
        },
      });

      return {
        ok: true,
        catalogTaskId: catalogTaskIdRaw,
        messageId: sendResult.messageId,
      };
    } catch (error) {
      const errorObj = error as TelegramError;
      const message = errorObj.message || 'Unknown catalog publish error';

      await prisma.catalogTask.update({
        where: { id: catalogTaskId },
        data: {
          status: CatalogTaskStatus.failed,
          finishedAt: new Date(),
          pinSuccess: null,
          pinErrorMessage: null,
          errorMessage: message,
        },
      });

      throw error;
    }
  },
  { connection, concurrency: 3 },
);

dispatchWorker.on('completed', (job) => {
  // eslint-disable-next-line no-console
  console.log(`[q_dispatch] completed job ${job.id}`);
});

dispatchWorker.on('failed', (job, err) => {
  // eslint-disable-next-line no-console
  console.error(`[q_dispatch] failed job ${job?.id}:`, err.message);
});

relayUploadWorker.on('completed', (job) => {
  // eslint-disable-next-line no-console
  console.log(`[q_relay_upload] completed job ${job.id}`);
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

  // eslint-disable-next-line no-console
  console.error(`[q_relay_upload] failed job ${job?.id}:`, err.message);
});

relayUploadWorker.on('error', (err) => {
  // eslint-disable-next-line no-console
  console.error('[q_relay_upload] worker error:', err);
});

catalogWorker.on('completed', (job) => {
  // eslint-disable-next-line no-console
  console.log(`[q_catalog] completed job ${job.id}`);
});

catalogWorker.on('failed', (job, err) => {
  // eslint-disable-next-line no-console
  console.error(`[q_catalog] failed job ${job?.id}:`, err.message);
});

async function drainStaleRelayJobs() {
  // eslint-disable-next-line no-console
  console.log('[bootstrap] draining stale relay upload jobs...');
  let removed = 0;

  const failedJobs = await relayUploadQueue.getFailed(0, 500);
  for (const job of failedJobs) {
    await job.remove();
    removed += 1;
  }

  const waitingJobs = await relayUploadQueue.getWaiting(0, 500);
  for (const job of waitingJobs) {
    if (job.name !== 'bootstrap-check') {
      await job.remove();
      removed += 1;
    }
  }

  const delayedJobs = await relayUploadQueue.getDelayed(0, 500);
  for (const job of delayedJobs) {
    await job.remove();
    removed += 1;
  }

  if (removed > 0) {
    // eslint-disable-next-line no-console
    console.log(`[bootstrap] removed ${removed} stale relay upload job(s)`);
  }
}

async function bootstrap() {
  await dispatchQueue.add(
    'bootstrap-check',
    { source: 'worker_startup', timestamp: new Date().toISOString() },
    { removeOnComplete: true, removeOnFail: 100 },
  );

  await catalogQueue.add(
    'bootstrap-check',
    { source: 'worker_startup', timestamp: new Date().toISOString() },
    { removeOnComplete: true, removeOnFail: 100 },
  );

  await relayUploadQueue.add(
    'bootstrap-check',
    { source: 'worker_startup', timestamp: new Date().toISOString() },
    { removeOnComplete: true, removeOnFail: 100 },
  );

  await drainStaleRelayJobs();

  setInterval(() => {
    void scheduleEnabledTaskDefinitions().catch((err) => {
      // eslint-disable-next-line no-console
      console.error('[scheduler:task-definitions] error:', err);
    });
  }, SCHEDULER_POLL_MS);

  // eslint-disable-next-line no-console
  console.log('Worker started. Queues: q_dispatch + q_relay_upload + q_catalog, task-definitions scheduler enabled');
}

bootstrap().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('Worker bootstrap error:', err);
  process.exit(1);
});

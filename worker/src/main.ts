import { Job, Queue, Worker } from 'bullmq';
import IORedis from 'ioredis';
import dotenv from 'dotenv';
import { createReadStream, openAsBlob } from 'node:fs';
import { mkdir, readdir, rename, stat } from 'node:fs/promises';
import { basename, dirname, extname, join, resolve } from 'node:path';
import { createHash } from 'node:crypto';
import {
  getTaskDefinitionLockKey as getLockKey,
  safeRunInterval,
} from './schedule-utils';
import { generateTextWithAiProfile } from './ai-provider';
import { logger, logError } from './logger';
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

const dispatchQueue = new Queue('q_dispatch', { connection: connection as any });
const catalogQueue = new Queue('q_catalog', { connection: connection as any });
const relayUploadQueue = new Queue('q_relay_upload', { connection: connection as any });

const SCHEDULER_POLL_MS = 5000;
const MAX_SCHEDULE_BATCH = 100;
const TASK_DEFINITION_LOCK_TTL_MS = Number(
  process.env.TASK_DEFINITION_LOCK_TTL_MS || '3600000',
);
const TASK_DEFINITION_ERROR_RETRY_SEC = Number(
  process.env.TASK_DEFINITION_ERROR_RETRY_SEC || '300',
);
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

/* ── §12 Observability: in-process metric counters ────────────────── */
const taskdefMetrics = {
  tickTotal: 0,
  dueTotal: 0,
  lockSkipTotal: 0,
  runSuccessTotal: 0,
  runFailedTotal: 0,
  runDurationMsTotal: 0,
};
const METRICS_LOG_INTERVAL_TICKS = 60; // ~5 min at 5s poll

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
  logger.info('[archive] moved file', { from: localPath, to: archivePath });
  return archivePath;
}

async function waitForFileStable(filePath: string) {
  let previousSize: number | null = null;
  let stableCount = 0;

  while (stableCount < RELAY_MIN_STABLE_CHECKS) {
    const s = await stat(filePath);

    if (previousSize !== null && Number(s.size) === previousSize) {
      stableCount += 1;
    } else {
      stableCount = 0;
      previousSize = Number(s.size);
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
  method: 'sendVideo' | 'sendDocument' | 'sendMessage' | 'pinChatMessage' | 'editMessageText';
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
    body: (isFormData ? args.payload : JSON.stringify(args.payload)) as any,
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
    messageId: result.messageId!,
    messageLink: result.messageId ? toTelegramMessageLink(args.chatId, result.messageId) : null,
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

async function editMessageTextByTelegram(args: {
  botToken: string;
  chatId: string;
  messageId: number;
  text: string;
  parseMode?: string;
}) {
  const result = await sendTelegramRequest({
    botToken: args.botToken,
    method: 'editMessageText',
    payload: {
      chat_id: args.chatId,
      message_id: args.messageId,
      text: args.text,
      parse_mode: args.parseMode ?? 'HTML',
      disable_web_page_preview: true,
    },
  });

  if (!result.messageId) {
    throw new Error('editMessageText response missing message_id');
  }
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
      lastRunSummary: args.summary as any,
    },
  });
}

async function createTaskRun(args: { taskDefinitionId: bigint; taskType: TaskDefinitionType }) {
  return prisma.taskRun.create({
    data: {
      taskDefinitionId: args.taskDefinitionId,
      taskType: args.taskType,
      status: 'running',
      startedAt: new Date(),
    },
    select: { id: true },
  });
}

async function finishTaskRun(args: {
  taskRunId: bigint;
  status: 'success' | 'failed';
  summary?: Record<string, unknown>;
  errorMessage?: string | null;
}) {
  await prisma.taskRun.update({
    where: { id: args.taskRunId },
    data: {
      status: args.status,
      finishedAt: new Date(),
      summary: args.summary as any,
      errorMessage: args.errorMessage ?? null,
    },
  });
}

async function createTaskRunStep(args: {
  taskRunId: bigint;
  entityType: 'media_asset' | 'dispatch_task' | 'catalog_task';
  entityId: bigint;
  title: string;
  payload?: Record<string, unknown>;
  status?: 'pending' | 'running' | 'success' | 'failed';
}) {
  return prisma.taskRunStep.create({
    data: {
      taskRunId: args.taskRunId,
      entityType: args.entityType,
      entityId: args.entityId,
      title: args.title,
      payload: args.payload as any,
      status: args.status ?? 'running',
      startedAt: (args.status === 'running' || !args.status) ? new Date() : null,
      finishedAt: args.status === 'success' || args.status === 'failed' ? new Date() : null,
    },
    select: { id: true },
  });
}

async function updateTaskRunStep(args: {
  taskRunId: bigint;
  entityType: 'media_asset' | 'dispatch_task' | 'catalog_task';
  entityId: bigint;
  status: 'running' | 'success' | 'failed';
  payload?: Record<string, unknown>;
}) {
  await prisma.taskRunStep.updateMany({
    where: {
      taskRunId: args.taskRunId,
      entityType: args.entityType,
      entityId: args.entityId,
    },
    data: {
      status: args.status,
      payload: args.payload as any,
      startedAt: args.status === 'running' ? new Date() : undefined,
      finishedAt: args.status === 'success' || args.status === 'failed' ? new Date() : undefined,
    },
  });
}

async function finishTaskRunStep(args: {
  taskRunStepId: bigint;
  status: 'success' | 'failed';
  payload?: Record<string, unknown>;
}) {
  await prisma.taskRunStep.update({
    where: { id: args.taskRunStepId },
    data: {
      status: args.status,
      payload: args.payload as any,
      finishedAt: new Date(),
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

async function enqueueRelayAssetsFromTaskDefinition(taskDefinitionId: bigint, taskRunId: bigint) {
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

      await createTaskRunStep({
        taskRunId,
        entityType: 'media_asset',
        entityId: asset.id,
        title: 'enqueue relay upload',
        payload: {
          channelId: channel.id.toString(),
          localPath: filePath,
          originalName: basename(filePath),
          status: asset.status,
        },
      });

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
            taskRunId: taskRunId.toString(),
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

async function scheduleDispatchForDefinition(taskDefinitionId: bigint, taskRunId: bigint) {
  try {
    const definition = await getTaskDefinitionModel().findUnique({
      where: { id: taskDefinitionId },
      select: { priority: true },
    });

    if (!definition) {
      throw new Error(`Task Definition ${taskDefinitionId} not found`);
    }

    // Auto-discover media assets that are ready for dispatch
    // but haven't been queued for dispatch yet.
    const unscheduledAssets = await prisma.mediaAsset.findMany({
      where: {
        status: MediaStatus.relay_uploaded,
        telegramFileId: { not: null },
        dispatchTasks: {
          none: {}, // Only pick assets that don't have any DispatchTasks
        },
      },
      select: {
        id: true,
        channelId: true,
      },
      take: 200, // Batch limit
    });

    let createdCount = 0;
    const now = new Date();

    for (const asset of unscheduledAssets) {
      const dispatchTask = await prisma.dispatchTask.create({
        data: {
          channelId: asset.channelId,
          mediaAssetId: asset.id,
          status: TaskStatus.pending,
          scheduleSlot: now,
          plannedAt: now,
          nextRunAt: now,
          priority: definition.priority ?? 100,
        },
        select: { id: true },
      });

      await createTaskRunStep({
        taskRunId,
        entityType: 'dispatch_task',
        entityId: dispatchTask.id,
        title: 'enqueue dispatch task',
        payload: {
          channelId: asset.channelId.toString(),
          mediaAssetId: asset.id.toString(),
          status: 'pending',
        },
      });

      createdCount++;
    }

    // Also call the base dispatch scheduler which actually queues them to BullMQ
    await scheduleDueDispatchTasks();

    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'success',
      summary: {
        executor: 'dispatch_send',
        createdTasks: createdCount,
        message: 'Auto-scanned and queued dispatch tasks',
      },
    });

    await finishTaskRun({
      taskRunId,
      status: 'success',
      summary: {
        executor: 'dispatch_send',
        createdTasks: createdCount,
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    // eslint-disable-next-line no-console
    console.error(`[scheduler] dispatch_send taskDef=${taskDefinitionId} failed:`, error);
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'failed',
      summary: { executor: 'dispatch_send', error: message },
    });

    await finishTaskRun({
      taskRunId,
      status: 'failed',
      errorMessage: message,
      summary: { executor: 'dispatch_send', error: message },
    });
  }
}

async function scheduleRelayForDefinition(taskDefinitionId: bigint, taskRunId: bigint) {
  try {
    const enqueueSummary = await enqueueRelayAssetsFromTaskDefinition(taskDefinitionId, taskRunId);
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

    await finishTaskRun({
      taskRunId,
      status: 'success',
      summary: {
        executor: 'relay_upload',
        ...enqueueSummary,
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

    await finishTaskRun({
      taskRunId,
      status: 'failed',
      errorMessage: message,
      summary: { executor: 'relay_upload', error: message },
    });

    throw error;
  }
}

async function scheduleCatalogForDefinition(taskDefinitionId: bigint, taskRunId: bigint) {
  try {
    await scheduleDueCatalogTasks(taskRunId);
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'success',
      summary: {
        executor: 'catalog_publish',
        message: 'catalog scheduler tick completed',
      },
    });

    await finishTaskRun({
      taskRunId,
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

    await finishTaskRun({
      taskRunId,
      status: 'failed',
      errorMessage: message,
      summary: { executor: 'catalog_publish', error: message },
    });

    throw error;
  }
}

function getTaskDefinitionLockKey(taskDefinitionId: bigint) {
  return getLockKey(taskDefinitionId);
}

async function tryAcquireTaskDefinitionLock(taskDefinitionId: bigint) {
  const lockToken = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const result = await connection.set(
    getTaskDefinitionLockKey(taskDefinitionId),
    lockToken,
    'PX',
    TASK_DEFINITION_LOCK_TTL_MS,
    'NX',
  );

  if (result !== 'OK') return null;
  return lockToken;
}

async function releaseTaskDefinitionLock(taskDefinitionId: bigint, lockToken: string) {
  const lockKey = getTaskDefinitionLockKey(taskDefinitionId);
  const lua = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
    end
    return 0
  `;

  await connection.eval(lua, 1, lockKey, lockToken);
}

async function scheduleTaskDefinitionByType(definition: {
  id: bigint;
  taskType: TaskDefinitionType;
  taskRunId: bigint;
}) {
  if (definition.taskType === TaskDefinitionType.relay_upload) {
    await scheduleRelayForDefinition(definition.id, definition.taskRunId);
    return;
  }

  if (definition.taskType === TaskDefinitionType.dispatch_send) {
    await scheduleDispatchForDefinition(definition.id, definition.taskRunId);
    return;
  }

  if (definition.taskType === TaskDefinitionType.catalog_publish) {
    await scheduleCatalogForDefinition(definition.id, definition.taskRunId);
  }
}

async function scheduleEnabledTaskDefinitions() {
  taskdefMetrics.tickTotal += 1;

  let definitions: Array<{
    id: bigint;
    taskType: TaskDefinitionType;
    runIntervalSec: number;
    nextRunAt: Date | null;
  }> = [];

  try {
    definitions = await getTaskDefinitionModel().findMany({
      where: {
        isEnabled: true,
        OR: [{ nextRunAt: null }, { nextRunAt: { lte: new Date() } }],
      },
      orderBy: [{ priority: 'asc' }, { nextRunAt: 'asc' }, { updatedAt: 'asc' }],
      take: MAX_SCHEDULE_BATCH,
      select: {
        id: true,
        taskType: true,
        runIntervalSec: true,
        nextRunAt: true,
      },
    });
  } catch (error) {
    const prismaCode =
      typeof error === 'object' && error !== null && 'code' in error
        ? (error as { code?: string }).code
        : undefined;

    const isSchemaNotReady = prismaCode === 'P2021' || prismaCode === 'P2022';

    if (isSchemaNotReady) {
      if (!hasWarnedMissingTaskDefinitionsTable) {
        logger.warn('[scheduler:task-definitions] task_definitions schema is not ready, fallback to legacy schedulers. Run prisma migrate to enable task-definition scheduling.');
        hasWarnedMissingTaskDefinitionsTable = true;
      }

      await scheduleDueDispatchTasks();
      await scheduleDueRelayUploadTasks();
      await scheduleDueCatalogTasks();
      return;
    }

    throw error;
  }

  taskdefMetrics.dueTotal += definitions.length;

  for (const definition of definitions) {
    const lockToken = await tryAcquireTaskDefinitionLock(definition.id);
    if (!lockToken) {
      taskdefMetrics.lockSkipTotal += 1;
      logger.info('[scheduler:taskdef] lock_skip', {
        taskDefinitionId: definition.id.toString(),
        taskType: definition.taskType,
      });
      continue;
    }

    const runStart = Date.now();
    const now = new Date();
    const safeRunIntervalSec = safeRunInterval(definition.runIntervalSec);
    const nextRunAtBefore = definition.nextRunAt?.toISOString() ?? null;
    let runStatus: 'success' | 'failed' = 'success';
    let nextRunAtAfter: string | null = null;
    let taskRunId: bigint | null = null;

    try {
      await getTaskDefinitionModel().update({
        where: { id: definition.id },
        data: {
          lastStartedAt: now,
        },
      });

      const taskRun = await createTaskRun({
        taskDefinitionId: definition.id,
        taskType: definition.taskType,
      });
      taskRunId = taskRun.id;

      await scheduleTaskDefinitionByType({
        ...definition,
        taskRunId: taskRun.id,
      });

      const newNextRunAt = new Date(now.getTime() + safeRunIntervalSec * 1000);
      nextRunAtAfter = newNextRunAt.toISOString();

      await getTaskDefinitionModel().update({
        where: { id: definition.id },
        data: {
          nextRunAt: newNextRunAt,
        },
      });

      taskdefMetrics.runSuccessTotal += 1;
    } catch (error) {
      runStatus = 'failed';
      taskdefMetrics.runFailedTotal += 1;

      const errorNextRunAt = new Date(
        now.getTime() +
        Math.min(safeRunIntervalSec, TASK_DEFINITION_ERROR_RETRY_SEC) * 1000,
      );
      nextRunAtAfter = errorNextRunAt.toISOString();

      await getTaskDefinitionModel().update({
        where: { id: definition.id },
        data: {
          nextRunAt: errorNextRunAt,
        },
      });

      if (taskRunId) {
        const message = error instanceof Error ? error.message : 'unknown error';
        await finishTaskRun({
          taskRunId,
          status: 'failed',
          errorMessage: message,
          summary: { executor: definition.taskType, error: message },
        });
      }

      logError('[scheduler:taskdef] run_failed', {
        taskDefinitionId: definition.id.toString(),
        taskType: definition.taskType,
        error: error instanceof Error ? error.message : 'unknown',
      });
    } finally {
      const durationMs = Date.now() - runStart;
      taskdefMetrics.runDurationMsTotal += durationMs;

      logger.info('taskdef_run', {
        tag: 'taskdef_run',
        taskDefinitionId: definition.id.toString(),
        taskType: definition.taskType,
        runIntervalSec: safeRunIntervalSec,
        lockAcquired: true,
        status: runStatus,
        durationMs,
        nextRunAtBefore,
        nextRunAtAfter,
      });

      await releaseTaskDefinitionLock(definition.id, lockToken);
    }
  }

  /* ── Periodic metrics dump (~every 5 min) ── */
  if (taskdefMetrics.tickTotal % METRICS_LOG_INTERVAL_TICKS === 0) {
    logger.info('taskdef_metrics', {
      tag: 'taskdef_metrics',
      ...taskdefMetrics,
      avgRunDurationMs:
        taskdefMetrics.runSuccessTotal + taskdefMetrics.runFailedTotal > 0
          ? Math.round(
            taskdefMetrics.runDurationMsTotal /
            (taskdefMetrics.runSuccessTotal + taskdefMetrics.runFailedTotal),
          )
          : 0,
    });
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
    logger.info('[scheduler] queued dispatch tasks', { count: dueTasks.length });
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
      originalName: true,
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

    const taskRunIdRaw = sourceMeta.taskRunId;
    const taskRunId = typeof taskRunIdRaw === 'string' ? BigInt(taskRunIdRaw) : null;

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

    if (taskRunId) {
      await updateTaskRunStep({
        taskRunId,
        entityType: 'media_asset',
        entityId: asset.id,
        status: 'running',
        payload: {
          channelId: asset.channelId.toString(),
          relayChannelId,
          mediaName: asset.originalName,
        },
      });
    }

    queuedCount += 1;
  }

  if (queuedCount > 0) {
    logger.info('[scheduler] queued relay upload tasks', { count: queuedCount });
  }
}

async function scheduleDueCatalogTasks(taskRunId?: bigint) {
  const now = new Date();

  const channels = await prisma.channel.findMany({
    where: {
      status: 'active',
      navEnabled: true,
    },
    select: {
      id: true,
      name: true,
      lastNavUpdateAt: true,
      navIntervalSec: true,
      navTemplateText: true,
      defaultBotId: true,
    },
  });

  let queuedCount = 0;

  for (const channel of channels) {
    if (!channel.navTemplateText || !channel.defaultBotId) continue;

    if (channel.lastNavUpdateAt) {
      const dueTime = channel.lastNavUpdateAt.getTime() + channel.navIntervalSec * 1000;
      if (now.getTime() < dueTime) continue;
    }

    const jobId = `catalog-${channel.id.toString()}`;
    const existingJob = await catalogQueue.getJob(jobId);
    if (existingJob) {
      const state = await existingJob.getState();
      if (state === 'failed') {
        await existingJob.remove();
      } else {
        continue;
      }
    }

    await catalogQueue.add(
      'catalog-publish',
      {
        channelIdRaw: channel.id.toString(),
        taskRunId: taskRunId ? taskRunId.toString() : undefined,
      },
      {
        jobId,
        removeOnComplete: true,
        removeOnFail: 200,
      },
    );

    if (taskRunId) {
      await createTaskRunStep({
        taskRunId,
        entityType: 'catalog_task',
        entityId: channel.id,
        title: 'catalog publish',
        payload: {
          channelId: channel.id.toString(),
          channelName: channel.name,
        },
      });
    }

    queuedCount += 1;
  }

  if (queuedCount > 0) {
    logger.info('[scheduler] queued catalog tasks', { count: queuedCount });
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
            name: true,
            tgChatId: true,
            defaultBotId: true,
            aiModelProfileId: true,
            aiSystemPromptTemplate: true,
            aiReplyMarkup: true,
          },
        },
        mediaAsset: {
          select: {
            id: true,
            telegramFileId: true,
            status: true,
            originalName: true,
            aiGeneratedCaption: true,
            sourceMeta: true,
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

    const taskRunIdRaw = (task.mediaAsset.sourceMeta as Record<string, unknown> | null)?.taskRunId;
    const taskRunId = typeof taskRunIdRaw === 'string' ? BigInt(taskRunIdRaw) : null;
    if (taskRunId) {
      await updateTaskRunStep({
        taskRunId,
        entityType: 'dispatch_task',
        entityId: dispatchTaskId,
        status: 'running',
        payload: {
          channelId: task.channelId.toString(),
          channelName: task.channel.name,
          mediaAssetId: task.mediaAsset.id.toString(),
          mediaName: task.mediaAsset.originalName,
        },
      });
    }

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

      let finalCaption = task.caption || task.mediaAsset.aiGeneratedCaption;

      if (!finalCaption && task.channel.aiSystemPromptTemplate) {
        let profile = task.channel.aiModelProfileId ? await prisma.aiModelProfile.findUnique({
          where: { id: task.channel.aiModelProfileId },
        }) : null;

        // Fallback to .env configuration if no database profile is bound to the channel, 
        // but a system prompt template IS provided and ENV vars are present.
        if (!profile && process.env.OPENAI_API_KEY) {
          profile = {
            id: BigInt(0),
            name: 'ENV_FALLBACK',
            provider: 'openai',
            model: process.env.OPENAI_MODEL || 'gpt-3.5-turbo',
            apiKeyEncrypted: process.env.OPENAI_API_KEY,
            endpointUrl: process.env.OPENAI_BASE_URL || null,
            systemPrompt: null,
            captionPromptTemplate: null,
            temperature: null,
            topP: null,
            maxTokens: null,
            timeoutMs: 20000,
            isActive: true,
            fallbackProfileId: null,
            createdAt: new Date(),
            updatedAt: new Date(),
          };
        }

        if (profile && profile.isActive) {
          try {
            finalCaption = await generateTextWithAiProfile(
              profile,
              task.channel.aiSystemPromptTemplate,
              `请为这个视频生成文案，原名：${task.mediaAsset.originalName}`
            );

            // Write generated caption back so we don't re-generate on retry
            await prisma.dispatchTask.update({
              where: { id: task.id },
              data: { caption: finalCaption }
            });
            // Also write back to mediaAsset so other tasks can use it
            await prisma.mediaAsset.update({
              where: { id: task.mediaAsset.id },
              data: { aiGeneratedCaption: finalCaption }
            });
          } catch (aiErr) {
                    logError('[q_dispatch] AI generation failed', { dispatchTaskId: task.id.toString(), error: aiErr });
            finalCaption = task.mediaAsset.originalName;
          }
        } else {
          finalCaption = task.mediaAsset.originalName;
        }
      } else if (!finalCaption) {
        finalCaption = task.mediaAsset.originalName;
      }

      if (!task.mediaAsset.aiGeneratedCaption && finalCaption) {
        await prisma.mediaAsset.update({
          where: { id: task.mediaAsset.id },
          data: { aiGeneratedCaption: finalCaption },
        });
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
        caption: finalCaption,
        parseMode: task.parseMode,
        replyMarkup: task.replyMarkup ?? task.channel.aiReplyMarkup ?? undefined,
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

      if (taskRunId) {
        await updateTaskRunStep({
          taskRunId,
          entityType: 'dispatch_task',
          entityId: dispatchTaskId,
          status: 'success',
          payload: {
            channelId: task.channelId.toString(),
            channelName: task.channel.name,
            mediaAssetId: task.mediaAsset.id.toString(),
            mediaName: task.mediaAsset.originalName,
            messageId: sendResult.messageId,
            messageLink: sendResult.messageLink,
          },
        });
      }

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

      if (taskRunId) {
        await updateTaskRunStep({
          taskRunId,
          entityType: 'dispatch_task',
          entityId: dispatchTaskId,
          status: 'failed',
          payload: {
            channelId: task.channelId.toString(),
            channelName: task.channel.name,
            mediaAssetId: task.mediaAsset.id.toString(),
            mediaName: task.mediaAsset.originalName,
            errorCode: code,
            errorMessage: message,
            retryCount: nextRetryCount,
          },
        });
      }

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
  { connection: connection as any, concurrency: 5 },
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

    const sourceMeta = (mediaAsset.sourceMeta ?? {}) as Record<string, unknown>;
    const taskRunIdRaw = sourceMeta.taskRunId;
    const taskRunId = typeof taskRunIdRaw === 'string' ? BigInt(taskRunIdRaw) : null;
    if (taskRunId) {
      await updateTaskRunStep({
        taskRunId,
        entityType: 'media_asset',
        entityId: mediaAssetId,
        status: 'success',
        payload: {
          channelId: mediaAsset.channelId.toString(),
          relayChannelId: relayChannelIdRaw,
          messageId: sendResult.messageId,
          mediaName: mediaAsset.originalName,
        },
      });
    }

    return {
      ok: true,
      mediaAssetId: mediaAssetIdRaw,
      relayChannelId: relayChannelIdRaw,
      messageId: sendResult.messageId,
    };
  },
  { connection: connection as any, concurrency: 2 },
);

const catalogWorker = new Worker(
  'q_catalog',
  async (job) => {
    if (job.name === 'bootstrap-check') {
      return { ok: true, skipped: true, reason: 'bootstrap-check' };
    }

    const channelIdRaw = job.data.channelIdRaw as string | undefined;
    const taskRunIdRaw = job.data.taskRunId as string | undefined;
    const taskRunId = taskRunIdRaw ? BigInt(taskRunIdRaw) : null;
    if (!channelIdRaw) {
      throw new Error('Missing channelIdRaw in job payload');
    }

    let catalogTaskId: bigint | null = null;

    const channelId = BigInt(channelIdRaw);

    const channel = await prisma.channel.findUnique({
      where: { id: channelId },
      include: {
        defaultBot: {
          select: { id: true, status: true, tokenEncrypted: true },
        },
      },
    });

    if (!channel) throw new Error(`Channel not found: ${channelIdRaw}`);
    if (!channel.navEnabled || channel.status !== 'active') {
      return { ok: true, skipped: true, reason: 'nav_disabled_or_not_active' };
    }
    if (!channel.navTemplateText || !channel.defaultBot) {
      throw new Error(`Missing navTemplateText or defaultBot for channel ${channelIdRaw}`);
    }
    if (channel.defaultBot.status !== 'active') {
      throw new Error(`Channel bot not active: ${channelIdRaw}`);
    }

    let catalogTemplate = await prisma.catalogTemplate.findFirst({
      where: { isActive: true },
      select: { id: true },
      orderBy: { createdAt: 'desc' },
    });

    if (!catalogTemplate) {
      catalogTemplate = await prisma.catalogTemplate.create({
        data: {
          name: '默认模板',
          bodyTemplate: '{{#each videos}}\\n{{this.short_title}}\\n{{this.message_url}}\\n{{/each}}',
          recentLimit: 60,
          isActive: true,
        },
        select: { id: true },
      });
    }

    // Scheme #1: create catalog_tasks record (pending -> running -> success/failed/cancelled)
    const now = new Date();
    const reuseWindowMs = 2 * 60 * 1000;
    const recentExisting = await prisma.catalogTask.findFirst({
      where: {
        channelId,
        status: { in: [CatalogTaskStatus.pending, CatalogTaskStatus.running] },
      },
      orderBy: { createdAt: 'desc' },
      select: { id: true, createdAt: true },
    });

    if (recentExisting && now.getTime() - recentExisting.createdAt.getTime() <= reuseWindowMs) {
      // Avoid duplicate inserts on job retries/bursts.
      catalogTaskId = recentExisting.id;
    } else {
      const created = await prisma.catalogTask.create({
        data: {
          channelId,
          catalogTemplateId: catalogTemplate.id,
          status: CatalogTaskStatus.pending,
          plannedAt: now,
          pinAfterPublish: false,
        },
        select: { id: true },
      });
      catalogTaskId = created.id;
    }

    await prisma.catalogTask.update({
      where: { id: catalogTaskId },
      data: { status: CatalogTaskStatus.running, startedAt: now },
    });

    const recentLimit = channel.navRecentLimit ?? 60;
    const dispatchTasks = await prisma.dispatchTask.findMany({
      where: { channelId, status: TaskStatus.success, telegramMessageId: { not: null } },
      orderBy: { finishedAt: 'desc' },
      take: recentLimit,
      select: {
        caption: true,
        telegramMessageLink: true,
      },
    });

    if (dispatchTasks.length === 0) {
      if (catalogTaskId) {
        await prisma.catalogTask.update({
          where: { id: catalogTaskId },
          data: {
            status: CatalogTaskStatus.cancelled,
            finishedAt: new Date(),
            errorMessage: 'no_successful_dispatch_tasks',
          },
        });
      }
      return { ok: true, skipped: true, reason: 'no_successful_dispatch_tasks' };
    }

    // Assemble videos list
    const videos = [...dispatchTasks].reverse().map((t) => {
      const parts = (t.caption || '').split('\n').map((l) => l.trim()).filter(Boolean);
      let shortTitle = '未命名视频';
      if (parts.length >= 2) {
        shortTitle = `${parts[0]} ${parts[1]}`;
      } else if (parts.length === 1) {
        shortTitle = parts[0];
      }
      return {
        message_url: t.telegramMessageLink || '',
        short_title: shortTitle,
      };
    });

    // Replace template
    let content = channel.navTemplateText;
    content = content.replace(/{{channel_name}}/g, channel.name);
    // basic html safe replacement for handlebars-like loop
    const eachRegex = /{{#each\s+videos}}([\s\S]*?){{\/each}}/g;
    content = content.replace(eachRegex, (match, body) => {
      return videos.map((v) => {
        let text = body.replace(/{{this\.message_url}}/g, v.message_url);
        text = text.replace(/{{this\.short_title}}/g, v.short_title);
        return text;
      }).join('');
    });

    // Add time
    const beijingTimeStr = new Date(Date.now() + 8 * 3600 * 1000).toISOString().replace('T', ' ').slice(0, 19);
    content = content.replace(/{{update_time}}/g, beijingTimeStr);

    const botToken = channel.defaultBot.tokenEncrypted;
    let finalMessageId: number | null = null;
    const contentPreview = content.slice(0, 4000);
    let pinAttempted = false;
    let pinSuccess: boolean | null = null;
    let pinErrorMessage: string | null = null;

    try {
      if (channel.navMessageId) {
        const oldMessageId = Number(channel.navMessageId);
        try {
          await editMessageTextByTelegram({
            botToken,
            chatId: channel.tgChatId,
            messageId: oldMessageId,
            text: content,
          });
          finalMessageId = oldMessageId;
        } catch (err) {
          const errObj = err as any;
          if (errObj.message && (errObj.message.includes('message to edit not found') || errObj.message.includes('message is not modified'))) {
            // If it's just not modified, we can still use it. But if not found, we send new.
            if (errObj.message.includes('not modified')) {
              finalMessageId = oldMessageId;
            } else {
              const sendResult = await sendTextByTelegram({ botToken, chatId: channel.tgChatId, text: content });
              finalMessageId = sendResult.messageId;
              pinAttempted = true;
              try {
                await pinMessageByTelegram({ botToken, chatId: channel.tgChatId, messageId: finalMessageId });
                pinSuccess = true;
              } catch (pinErr) {
                pinSuccess = false;
                pinErrorMessage = pinErr instanceof Error ? pinErr.message : 'pin failed';
              }
            }
          } else {
            throw err;
          }
        }
      } else {
        const sendResult = await sendTextByTelegram({ botToken, chatId: channel.tgChatId, text: content });
        finalMessageId = sendResult.messageId;
        pinAttempted = true;
        try {
          await pinMessageByTelegram({ botToken, chatId: channel.tgChatId, messageId: finalMessageId });
          pinSuccess = true;
        } catch (pinErr) {
          pinSuccess = false;
          pinErrorMessage = pinErr instanceof Error ? pinErr.message : 'pin failed';
        }
      }

      await prisma.channel.update({
        where: { id: channelId },
        data: {
          navMessageId: finalMessageId ? BigInt(finalMessageId) : null,
          lastNavUpdateAt: new Date(),
        },
      });

      await prisma.catalogHistory.create({
        data: {
          channelId,
          catalogTemplateId: catalogTemplate.id,
          content,
          renderedCount: videos.length,
          publishedAt: new Date(),
        },
      });

      if (catalogTaskId) {
        await prisma.catalogTask.update({
          where: { id: catalogTaskId },
          data: {
            status: CatalogTaskStatus.success,
            finishedAt: new Date(),
            telegramMessageId: finalMessageId ? BigInt(finalMessageId) : null,
            contentPreview,
            errorMessage: null,
            pinAfterPublish: pinAttempted,
            pinSuccess,
            pinErrorMessage,
          },
        });
      }

      if (taskRunId) {
        await updateTaskRunStep({
          taskRunId,
          entityType: 'catalog_task',
          entityId: channelId,
          status: 'success',
          payload: {
            channelId: channelIdRaw,
            channelName: channel.name,
            messageId: finalMessageId,
            status: 'success',
          },
        });
      }

      return {
        ok: true,
        channelId: channelIdRaw,
        messageId: finalMessageId,
      };
    } catch (error) {
      if (catalogTaskId) {
        await prisma.catalogTask.update({
          where: { id: catalogTaskId },
          data: {
            status: CatalogTaskStatus.failed,
            finishedAt: new Date(),
            contentPreview,
            errorMessage: error instanceof Error ? error.message : 'catalog publish failed',
            pinAfterPublish: pinAttempted,
            pinSuccess,
            pinErrorMessage: pinErrorMessage ?? (error instanceof Error ? error.message : 'catalog publish failed'),
          },
        });
      }

      if (taskRunId) {
        await updateTaskRunStep({
          taskRunId,
          entityType: 'catalog_task',
          entityId: channelId,
          status: 'failed',
          payload: {
            channelId: channelIdRaw,
            channelName: channel.name,
            errorMessage: error instanceof Error ? error.message : 'catalog publish failed',
          },
        });
      }

      logError('[q_catalog] publish error', {
        channelId: channelIdRaw,
        error,
      });
      throw error;
    }
  },
  { connection: connection as any, concurrency: 3 },
);

dispatchWorker.on('completed', (job) => {
  logger.info('[q_dispatch] completed job', { jobId: String(job.id) });
});

dispatchWorker.on('failed', (job, err) => {
  logError('[q_dispatch] failed job', {
    jobId: job?.id ? String(job.id) : null,
    error: err,
  });
});

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

  const mediaAsset = mediaAssetIdRaw
    ? await prisma.mediaAsset.findUnique({
      where: { id: BigInt(mediaAssetIdRaw) },
      select: { sourceMeta: true, channelId: true, originalName: true },
    })
    : null;
  const taskRunIdRaw = (mediaAsset?.sourceMeta as Record<string, unknown> | null)?.taskRunId;
  const taskRunId = typeof taskRunIdRaw === 'string' ? BigInt(taskRunIdRaw) : null;
  if (taskRunId && mediaAssetIdRaw) {
    await updateTaskRunStep({
      taskRunId,
      entityType: 'media_asset',
      entityId: BigInt(mediaAssetIdRaw),
      status: 'failed',
      payload: {
        channelId: mediaAsset?.channelId?.toString() ?? null,
        mediaName: mediaAsset?.originalName ?? null,
        relayChannelId: job?.data?.relayChannelId ?? null,
        errorMessage: err?.message ?? 'relay upload failed',
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

catalogWorker.on('completed', (job) => {
  logger.info('[q_catalog] completed job', { jobId: String(job.id) });
});

catalogWorker.on('failed', (job, err) => {
  logError('[q_catalog] failed job', {
    jobId: job?.id ? String(job.id) : null,
    error: err,
  });
});

async function drainStaleRelayJobs() {
  logger.info('[bootstrap] draining stale relay upload jobs...');
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
    logger.info('[bootstrap] removed stale relay upload jobs', { count: removed });
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
      logError('[scheduler:task-definitions] error', err);
    });
  }, SCHEDULER_POLL_MS);

  logger.info('Worker started. Queues: q_dispatch + q_relay_upload + q_catalog, task-definitions scheduler enabled');
}

bootstrap().catch((err) => {
  logError('Worker bootstrap error', err);
  process.exit(1);
});

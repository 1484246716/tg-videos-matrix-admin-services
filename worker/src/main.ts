import { Queue, Worker } from 'bullmq';
import IORedis from 'ioredis';
import dotenv from 'dotenv';
import { CatalogTaskStatus, PrismaClient, TaskStatus } from '@prisma/client';

dotenv.config({ path: '../../.env' });
dotenv.config();

const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
const telegramApiBase = process.env.TELEGRAM_BOT_API_BASE || 'https://api.telegram.org';

const connection = new IORedis(redisUrl, { maxRetriesPerRequest: null });
const prisma = new PrismaClient();

const dispatchQueue = new Queue('q_dispatch', { connection });
const catalogQueue = new Queue('q_catalog', { connection });

const SCHEDULER_POLL_MS = 5000;
const MAX_SCHEDULE_BATCH = 100;

function getBackoffSeconds(retryCount: number): number {
  const base = Math.max(1, Math.pow(2, retryCount));
  return Math.min(base * 30, 3600);
}

function normalizeTelegramApiBase(raw: string): string {
  return raw.replace(/\/+$/, '');
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
  method: 'sendVideo' | 'sendMessage' | 'pinChatMessage';
  payload: Record<string, unknown>;
}): Promise<{ messageId?: number }> {
  const endpoint = `${normalizeTelegramApiBase(telegramApiBase)}/bot${args.botToken}/${args.method}`;

  const response = await fetch(endpoint, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
    },
    body: JSON.stringify(args.payload),
  });

  const json = (await response.json()) as {
    ok: boolean;
    result?: { message_id?: number } | true;
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
        jobId: `dispatch:${task.id.toString()}`,
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
        jobId: `catalog:${task.id.toString()}`,
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
        caption: task.captionText,
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

const catalogWorker = new Worker(
  'q_catalog',
  async (job) => {
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

catalogWorker.on('completed', (job) => {
  // eslint-disable-next-line no-console
  console.log(`[q_catalog] completed job ${job.id}`);
});

catalogWorker.on('failed', (job, err) => {
  // eslint-disable-next-line no-console
  console.error(`[q_catalog] failed job ${job?.id}:`, err.message);
});

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

  setInterval(() => {
    void scheduleDueDispatchTasks().catch((err) => {
      // eslint-disable-next-line no-console
      console.error('[scheduler:dispatch] error:', err);
    });

    void scheduleDueCatalogTasks().catch((err) => {
      // eslint-disable-next-line no-console
      console.error('[scheduler:catalog] error:', err);
    });
  }, SCHEDULER_POLL_MS);

  // eslint-disable-next-line no-console
  console.log('Worker started. Queues: q_dispatch + q_catalog, scheduler enabled');
}

bootstrap().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('Worker bootstrap error:', err);
  process.exit(1);
});

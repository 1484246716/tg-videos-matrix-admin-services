import { Worker, Job } from 'bullmq';
import { connection } from '../infra/redis';
import { prisma } from '../infra/prisma';
import { logger, logError } from '../logger';
import { sendTelegramRequest } from '../shared/telegram';
import { MediaStatus } from '@prisma/client';

type BackfillJobPayload = {
  mediaAssetId: string;
  chatId: string;
  messageId?: string;
  fileName?: string;
};

// 修复点：强制转换为 BigInt，防止 Prisma 类型校验报错崩溃
async function resolveBotToken(chatId: string): Promise<string> {
  const relayChannel = await prisma.relayChannel.findFirst({
    where: { tgChatId: BigInt(chatId) },
    include: { bot: { select: { status: true, tokenEncrypted: true } } },
  });

  if (!relayChannel || !relayChannel.bot || relayChannel.bot.status !== 'active') {
    throw new Error(`回查机器人不存在或未启用，chat=${chatId}`);
  }

  return relayChannel.bot.tokenEncrypted;
}

// 透传 botToken，无需每次删除再查库
async function safeDeleteMessage(chatId: string, messageId: number, botToken: string) {
  try {
    await sendTelegramRequest({
      botToken,
      method: 'deleteMessage',
      payload: { chat_id: chatId, message_id: messageId },
    });
    logger.info(`[q_relay_fileid_backfill] 已自动清理临时转发消息`, { messageId });
  } catch (err) {
    logError('[q_relay_fileid_backfill] 删除临时转发消息失败', err);
  }
}

async function handleBackfillJob(job: Job<BackfillJobPayload>) {
  const mediaAssetId = BigInt(job.data.mediaAssetId);
  const chatId = job.data.chatId;
  const messageId = job.data.messageId ? Number(job.data.messageId) : undefined;

  logger.info(`[q_relay_fileid_backfill] 开始处理大文件回查任务`, {
    mediaAssetId: job.data.mediaAssetId,
    messageId: messageId ?? null,
    fileName: job.data.fileName ?? null,
  });

  try {
    const botToken = await resolveBotToken(chatId);

    let finalFileId: string | undefined;
    let finalFileUniqueId: string | undefined;
    let tempMessageId: number | undefined;

    if (messageId) {
      const forwarded = await sendTelegramRequest({
        botToken,
        method: 'forwardMessage',
        payload: { chat_id: chatId, from_chat_id: chatId, message_id: messageId },
      });

      finalFileId = forwarded.videoFileId ?? forwarded.documentFileId ?? forwarded.animationFileId;
      finalFileUniqueId = forwarded.videoFileUniqueId ?? forwarded.documentFileUniqueId ?? forwarded.animationFileUniqueId;
      tempMessageId = forwarded.messageId;

      if (!finalFileId) {
        if (tempMessageId) {
          await safeDeleteMessage(chatId, tempMessageId, botToken);
        }
        throw new Error(`消息 ${messageId} 仍在后台转码中，稍后重试`);
      }
    } else {
      const updatesResult = await sendTelegramRequest({
        botToken,
        method: 'getUpdates',
        payload: {
          timeout: 0,
          limit: 50,
          allowed_updates: ['message', 'channel_post'],
        },
      });

      const updates = updatesResult.updates ?? [];
      const expectedFileName = job.data.fileName?.toLowerCase() ?? null;

      for (const update of updates) {
        const payload = update.channel_post ?? update.message;
        if (!payload || !payload.chat || payload.chat.id?.toString() !== chatId) continue;

        const candidateFileName = payload.document?.file_name ?? payload.video?.file_name;
        if (expectedFileName && candidateFileName?.toLowerCase() !== expectedFileName) continue;

        if (payload.message_id) {
          const forwarded = await sendTelegramRequest({
            botToken,
            method: 'forwardMessage',
            payload: { chat_id: chatId, from_chat_id: chatId, message_id: payload.message_id },
          });

          finalFileId = forwarded.videoFileId ?? forwarded.documentFileId ?? forwarded.animationFileId;
          finalFileUniqueId = forwarded.videoFileUniqueId ?? forwarded.documentFileUniqueId ?? forwarded.animationFileUniqueId;
          tempMessageId = forwarded.messageId;
          if (finalFileId) break;
          if (tempMessageId) {
            await safeDeleteMessage(chatId, tempMessageId, botToken);
          }
        }
      }

      if (!finalFileId) {
        throw new Error('未在最近消息中找到目标文件或仍在转码中');
      }
    }

    await prisma.mediaAsset.update({
      where: { id: mediaAssetId },
      data: {
        telegramFileId: finalFileId,
        telegramFileUniqueId: finalFileUniqueId ?? null,
        status: MediaStatus.relay_uploaded,
      },
    });

    if (tempMessageId) {
      await safeDeleteMessage(chatId, tempMessageId, botToken);
    }

    logger.info(`[q_relay_fileid_backfill] ID 提取成功`, { finalFileId });
    return { ok: true, fileId: finalFileId };
  } catch (error: any) {
    const responseCode = error?.code?.startsWith('TG_') ? Number(error.code.replace('TG_', '')) : undefined;

    if (responseCode === 400) {
      await prisma.mediaAsset.update({
        where: { id: mediaAssetId },
        data: { status: MediaStatus.failed, ingestError: '源消息丢失或被删除 (400 错误)' },
      });
      return;
    }

    if (responseCode === 429) {
      const waitTime = error?.retryAfterSec ?? 30;
      logger.warn('[q_relay_fileid_backfill] 触发限流，稍后重试', { waitTime });
      throw new Error(`触发限流 429，${waitTime}s 后重试`);
    }

    throw error;
  }
}

export const backfillWorker = new Worker<BackfillJobPayload>(
  'q_relay_fileid_backfill',
  handleBackfillJob,
  {
    connection: connection as any,
    concurrency: 1, // 必须为 1，保护 Telegram API 防止 429
    limiter: { max: 1, duration: 2000 },
  },
);

backfillWorker.on('completed', (job) => {
  logger.info('[q_relay_fileid_backfill] 任务完成', { jobId: String(job.id) });
});

backfillWorker.on('failed', async (job, err) => {
  if (!job) return;
  const mediaAssetIdRaw = job.data?.mediaAssetId;
  if (mediaAssetIdRaw && job.attemptsMade >= (job.opts.attempts ?? 0)) {
    await prisma.mediaAsset.update({
      where: { id: BigInt(mediaAssetIdRaw) },
      data: { status: MediaStatus.failed, ingestError: `大文件提取最终超时或失败: ${err.message}` },
    });
  }

  logError('[q_relay_fileid_backfill] 任务失败', {
    jobId: job?.id ? String(job.id) : null,
    mediaAssetId: mediaAssetIdRaw ?? null,
    error: err,
  });
});

backfillWorker.on('error', (err) => {
  logError('[q_relay_fileid_backfill] Worker 异常', err);
});
import { Worker, Job } from 'bullmq';
import { connection } from '../infra/redis';
import { prisma } from '../infra/prisma';
import { logger, logError } from '../logger';
import { sendTelegramRequest } from '../shared/telegram';
import { MediaStatus } from '@prisma/client';

type BackfillJobPayload = {
  mediaAssetId: string;
  chatId: string;
  messageId: string;
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
  const messageId = Number(job.data.messageId);

  logger.info(`[q_relay_fileid_backfill] 开始处理大文件回查任务`, { mediaAssetId: job.data.mediaAssetId, messageId });

  try {
    const botToken = await resolveBotToken(chatId);

    const forwarded = await sendTelegramRequest({
      botToken,
      method: 'forwardMessage',
      payload: { chat_id: chatId, from_chat_id: chatId, message_id: messageId },
    });

    const finalFileId = forwarded.videoFileId ?? forwarded.documentFileId ?? forwarded.animationFileId;

    if (!finalFileId || !forwarded.messageId) {
      if (forwarded.messageId) {
        await safeDeleteMessage(chatId, forwarded.messageId, botToken);
      }
      throw new Error(`消息 ${messageId} 仍在后台转码中，稍后重试`);
    }

    await prisma.mediaAsset.update({
      where: { id: mediaAssetId },
      data: {
        telegramFileId: finalFileId,
        telegramFileUniqueId: forwarded.videoFileUniqueId ?? forwarded.documentFileUniqueId ?? forwarded.animationFileUniqueId ?? null,
        status: MediaStatus.relay_uploaded,
      },
    });

    await safeDeleteMessage(chatId, forwarded.messageId, botToken);

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
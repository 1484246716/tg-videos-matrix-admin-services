import { generateTextWithAiProfile } from '../ai-provider';
import { logError } from '../logger';
import { prisma } from '../infra/prisma';
import { sendVideoByTelegram, TelegramError } from '../shared/telegram';
import { getBackoffSeconds } from '../shared/dispatch-utils';
import { TaskStatus } from '@prisma/client';

export async function handleDispatchJob(
  dispatchTaskIdRaw: string,
  jobId: string,
  attemptsMade: number,
) {
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

  await prisma.dispatchTaskLog.create({
    data: {
      dispatchTaskId,
      action: 'task_running',
      detail: {
        jobId,
        attemptsMade,
      },
    },
  });

  try {
    if (!task.mediaAsset.telegramFileId) {
      throw new Error('Media asset has no telegramFileId (relay not uploaded)');
    }

    let finalCaption = task.caption || task.mediaAsset.aiGeneratedCaption;

    if (!finalCaption && task.channel.aiSystemPromptTemplate) {
      let profile = task.channel.aiModelProfileId
        ? await prisma.aiModelProfile.findUnique({
            where: { id: task.channel.aiModelProfileId },
          })
        : null;

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
            `请为这个视频生成文案，原名：${task.mediaAsset.originalName}`,
          );

          await prisma.dispatchTask.update({
            where: { id: task.id },
            data: { caption: finalCaption },
          });
          await prisma.mediaAsset.update({
            where: { id: task.mediaAsset.id },
            data: { aiGeneratedCaption: finalCaption },
          });
        } catch (aiErr) {
          logError('[q_dispatch] AI generation failed', {
            dispatchTaskId: task.id.toString(),
            error: aiErr,
          });
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
            jobId,
          },
        },
      });
    }

    throw error;
  }
}

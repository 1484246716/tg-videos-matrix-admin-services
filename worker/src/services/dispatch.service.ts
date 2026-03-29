import { TaskStatus } from '@prisma/client';
import { generateTextWithAiProfile } from '../ai-provider';
import { DISPATCH_CHANNEL_INTERVAL_GUARD_ENABLED } from '../config/env';
import { prisma } from '../infra/prisma';
import { searchIndexQueue } from '../infra/redis';
import { logger, logError } from '../logger';
import { getBackoffSeconds } from '../shared/dispatch-utils';
import { sendVideoByTelegram, TelegramError } from '../shared/telegram';

function isParseEntitiesError(error: { code?: string; message?: string } | null | undefined) {
  const message = (error?.message ?? '').toLowerCase();
  const code = error?.code ?? '';

  return (
    code === 'TG_400' &&
    (message.includes("can't parse entities") ||
      message.includes('unsupported start tag') ||
      message.includes('unsupported end tag') ||
      message.includes('entity not found') ||
      (message.includes('tag') && message.includes('not closed')))
  );
}

function isDeterministicDispatchError(error: { code?: string; message?: string } | null | undefined) {
  const message = (error?.message ?? '').toLowerCase();

  return (
    isParseEntitiesError(error) ||
    message.includes('媒体资源缺少 telegramfileid') ||
    message.includes('分发任务或频道未配置机器人') ||
    message.includes('未找到可用机器人')
  );
}

function getFileStem(fileName: string) {
  const trimmed = fileName.trim();
  const stem = trimmed.replace(/\.[^./\\]+$/, '').trim();
  return stem || trimmed;
}

function isAiFailureText(text?: string | null) {
  if (!text) return false;
  const normalized = text.trim();
  if (!normalized) return false;

  return /无法识别|抱歉|如果可以提供更多|视频的内容简介|主要角色|生成相关文案/.test(normalized);
}

function getCollectionDisplayName(name: string) {
  const normalized = name.replace(/合集/g, '').trim();
  return normalized || name.trim();
}

function buildCollectionEpisodeTitle(collectionName: string, episodeNo: number) {
  return `${getCollectionDisplayName(collectionName)}第${episodeNo}集`;
}

function applyCollectionEpisodeTitle(caption: string, title: string) {
  const desiredLine = `📺片名：${title}`;
  const trimmedCaption = caption.trim();
  if (!trimmedCaption) return desiredLine;

  if (/(?:^|\n)\s*📺?\s*片名\s*[：:]\s*.+/.test(trimmedCaption)) {
    return trimmedCaption.replace(/(^|\n)\s*📺?\s*片名\s*[：:]\s*.+/, (_match, prefix: string) => `${prefix}${desiredLine}`);
  }

  return `${desiredLine}\n${trimmedCaption}`;
}

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
          postIntervalSec: true,
          lastPostAt: true,
        },
      },
      mediaAsset: {
        select: {
          id: true,
          telegramFileId: true,
          status: true,
          originalName: true,
          aiGeneratedCaption: true,
          durationSec: true,
          sourceMeta: true,
        },
      },
    },
  });

  if (!task) {
    throw new Error(`未找到分发任务: ${dispatchTaskIdRaw}`);
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
      throw new Error('媒体资源缺少 telegramFileId（中转尚未完成）');
    }

    const mediaSourceMeta =
      task.mediaAsset.sourceMeta && typeof task.mediaAsset.sourceMeta === 'object'
        ? (task.mediaAsset.sourceMeta as Record<string, unknown>)
        : null;
    const originalNameStem = getFileStem(task.mediaAsset.originalName);
    const runtimeHint =
      typeof task.mediaAsset.durationSec === 'number' && task.mediaAsset.durationSec > 0
        ? `视频实测时长约 ${Math.floor(task.mediaAsset.durationSec / 60)} 分 ${task.mediaAsset.durationSec % 60} 秒（请据此填写“单集片长”，不要瞎编）`
        : '未探测到可靠视频时长（“单集片长”请谨慎表述为未知或约略，不要乱填具体分钟数）';

    let finalCaption = task.caption || task.mediaAsset.aiGeneratedCaption;
    if (isAiFailureText(finalCaption)) {
      finalCaption = originalNameStem;
    }

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
            `请为这个视频生成文案，原名：${task.mediaAsset.originalName}\n${runtimeHint}`,
          );

          if (isAiFailureText(finalCaption)) {
            finalCaption = originalNameStem;
          }

          await prisma.dispatchTask.update({
            where: { id: task.id },
            data: { caption: finalCaption },
          });
          await prisma.mediaAsset.update({
            where: { id: task.mediaAsset.id },
            data: { aiGeneratedCaption: finalCaption },
          });
        } catch (aiErr) {
          logError('[q_dispatch] AI 文案生成失败', {
            dispatchTaskId: task.id.toString(),
            error: aiErr,
          });
          finalCaption = originalNameStem;
        }
      } else {
        finalCaption = originalNameStem;
      }
    } else if (!finalCaption) {
      finalCaption = originalNameStem;
    }

    if (isAiFailureText(finalCaption)) {
      finalCaption = originalNameStem;
    }

    const isCollectionAsset = mediaSourceMeta?.isCollection === true;
    const collectionName = typeof mediaSourceMeta?.collectionName === 'string' ? mediaSourceMeta.collectionName.trim() : '';
    const episodeNo =
      typeof mediaSourceMeta?.episodeNo === 'number'
        ? mediaSourceMeta.episodeNo
        : typeof mediaSourceMeta?.episodeNo === 'string' && /^\d+$/.test(mediaSourceMeta.episodeNo)
          ? Number(mediaSourceMeta.episodeNo)
          : null;
    if (isCollectionAsset && collectionName && episodeNo !== null) {
      finalCaption = applyCollectionEpisodeTitle(
        finalCaption || '',
        buildCollectionEpisodeTitle(collectionName, episodeNo),
      );
    }

    if (!task.mediaAsset.aiGeneratedCaption && finalCaption) {
      await prisma.mediaAsset.update({
        where: { id: task.mediaAsset.id },
        data: { aiGeneratedCaption: finalCaption },
      });
    }

    const sourceRelayBotIdRaw = mediaSourceMeta?.relayBotId;
    const sourceRelayBotId =
      typeof sourceRelayBotIdRaw === 'string' && /^\d+$/.test(sourceRelayBotIdRaw)
        ? BigInt(sourceRelayBotIdRaw)
        : null;

    const resolvedBotId = sourceRelayBotId ?? task.botId ?? task.channel.defaultBotId;
    if (!resolvedBotId) {
      throw new Error('分发任务或频道未配置机器人');
    }

    const bot = await prisma.bot.findFirst({
      where: {
        id: resolvedBotId,
        status: 'active',
      },
      select: { id: true, tokenEncrypted: true },
    });

    if (!bot) {
      throw new Error(
        `未找到可用机器人: dispatchTaskId=${task.id.toString()}, resolvedBotId=${resolvedBotId.toString()}`,
      );
    }

    if (DISPATCH_CHANNEL_INTERVAL_GUARD_ENABLED) {
      const now = new Date();
      const intervalSec = Math.max(0, task.channel.postIntervalSec ?? 0);
      const nextAllowedAt = task.channel.lastPostAt
        ? new Date(task.channel.lastPostAt.getTime() + intervalSec * 1000)
        : now;

      if (nextAllowedAt.getTime() > now.getTime()) {
        await prisma.dispatchTask.update({
          where: { id: dispatchTaskId },
          data: {
            status: TaskStatus.scheduled,
            nextRunAt: nextAllowedAt,
            finishedAt: now,
          },
        });

        await prisma.dispatchTaskLog.create({
          data: {
            dispatchTaskId,
            action: 'task_deferred_by_channel_interval',
            detail: {
              channelId: task.channelId.toString(),
              postIntervalSec: intervalSec,
              lastPostAt: task.channel.lastPostAt?.toISOString() ?? null,
              nextAllowedAt: nextAllowedAt.toISOString(),
            },
          },
        });

        logger.info('[q_dispatch] 任务延后（未到频道发送窗口）', {
          dispatchTaskId: dispatchTaskIdRaw,
          channelId: task.channelId.toString(),
          postIntervalSec: intervalSec,
          lastPostAt: task.channel.lastPostAt?.toISOString() ?? null,
          nextAllowedAt: nextAllowedAt.toISOString(),
        });

        return {
          ok: true,
          skipped: true,
          reason: 'channel_interval_not_due',
          dispatchTaskId: dispatchTaskIdRaw,
        };
      }
    }

    let sendResult;
    try {
      sendResult = await sendVideoByTelegram({
        botToken: bot.tokenEncrypted,
        chatId: task.channel.tgChatId,
        fileId: task.mediaAsset.telegramFileId,
        caption: finalCaption,
        parseMode: task.parseMode,
        replyMarkup: task.replyMarkup ?? task.channel.aiReplyMarkup ?? undefined,
      });
    } catch (sendError) {
      const errorObj = sendError as TelegramError;
      if (
        task.parseMode?.toUpperCase() === 'HTML' &&
        isParseEntitiesError({ code: errorObj.code, message: errorObj.message })
      ) {
        logger.warn('[q_dispatch] HTML 解析失败，回退纯文本重发', {
          dispatchTaskId: dispatchTaskIdRaw,
          channelId: task.channelId.toString(),
          errorCode: errorObj.code,
          errorMessage: errorObj.message,
        });

        sendResult = await sendVideoByTelegram({
          botToken: bot.tokenEncrypted,
          chatId: task.channel.tgChatId,
          fileId: task.mediaAsset.telegramFileId,
          caption: finalCaption,
          parseMode: null,
          replyMarkup: task.replyMarkup ?? task.channel.aiReplyMarkup ?? undefined,
        });
      } else {
        throw sendError;
      }
    }

    await prisma.$transaction([
      prisma.dispatchTask.update({
        where: { id: dispatchTaskId },
        data: {
          status: TaskStatus.success,
          finishedAt: new Date(),
          botId: bot.id,
          telegramMessageId: BigInt(sendResult.messageId),
          telegramMessageLink: sendResult.messageLink,
          telegramErrorCode: null,
          telegramErrorMessage: null,
        },
      }),
      prisma.channel.update({
        where: { id: task.channelId },
        data: { lastPostAt: new Date() },
      }),
    ]);

    await prisma.dispatchTaskLog.create({
      data: {
        dispatchTaskId,
        action: 'task_success',
        detail: {
          botId: bot.id.toString(),
          messageId: sendResult.messageId,
          messageLink: sendResult.messageLink,
        },
      },
    });

    // ── 触发搜索索引更新（延迟2秒等AI caption等后续处理完成）──
    try {
      await searchIndexQueue.add('upsert', {
        sourceType: 'dispatch_task',
        sourceId: task.id.toString(),
        mediaAssetId: task.mediaAsset.id.toString(),
        channelId: task.channelId.toString(),
      }, {
        delay: 2000,
        jobId: `search-index-asset-${task.mediaAsset.id}`,
      });
    } catch (indexErr) {
      // 搜索索引失败不阻塞主流程
      logError('[q_dispatch] 搜索索引入队失败（不阻塞）', { error: indexErr });
    }

    return {
      ok: true,
      dispatchTaskId: dispatchTaskIdRaw,
      messageId: sendResult.messageId,
    };

  } catch (error) {
    const nextRetryCount = task.retryCount + 1;
    const now = new Date();

    const errorObj = error as TelegramError;
    const message = errorObj.message || '未知分发错误';
    const code = errorObj.code || 'DISPATCH_ERROR';

    const retryAfterSec = errorObj.retryAfterSec;
    const fallbackBackoffSec = getBackoffSeconds(nextRetryCount);
    const finalBackoffSec = retryAfterSec ?? fallbackBackoffSec;
    const nextRunAt = new Date(Date.now() + finalBackoffSec * 1000);

    const exceeded = nextRetryCount > task.maxRetries;
    const deterministic = isDeterministicDispatchError({ code, message });

    const nextStatus = exceeded || deterministic ? TaskStatus.dead : TaskStatus.failed;

    await prisma.dispatchTask.update({
      where: { id: dispatchTaskId },
      data: {
        status: nextStatus,
        retryCount: nextRetryCount,
        nextRunAt: nextStatus === TaskStatus.dead ? task.nextRunAt : nextRunAt,
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
          nextRunAt: nextStatus === TaskStatus.dead ? null : nextRunAt,
          deterministic,
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

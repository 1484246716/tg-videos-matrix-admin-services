import { getBackoffSeconds } from '../shared/dispatch-utils';
import { prisma } from '../infra/prisma';
import { logger } from '../logger';
import type { TelegramError } from '../shared/telegram';
import {
  pinMessageByTelegram,
  sendPhotoByTelegram,
  sendTextByTelegram,
  sendVideoByTelegram,
  unpinMessageByTelegram,
} from '../shared/telegram';

function resolveParseMode(format: 'markdown' | 'html' | 'plain' | null | undefined) {
  if (!format) return 'HTML';
  if (format === 'html') return 'HTML';
  if (format === 'markdown') return 'MarkdownV2';
  return undefined;
}

function normalizeReplyMarkup(input: unknown) {
  if (!input) return undefined;

  const toKeyboardRows = (value: unknown): unknown[][] | null => {
    if (!Array.isArray(value)) return null;

    // Already rows
    if (value.length === 0) return [];
    if (Array.isArray(value[0])) return value as unknown[][];

    // UI: [{ buttons: [...] }]
    if (typeof value[0] === 'object' && value[0] !== null && 'buttons' in (value[0] as any)) {
      const rows = (value as Array<{ buttons?: unknown }>).map((row) =>
        Array.isArray(row.buttons) ? row.buttons : [],
      );
      return rows;
    }

    // Flat array => wrap as single row
    return [value as unknown[]];
  };

  const normalizeRows = (rows: unknown[][]): unknown[][] => {
    return rows
      .map((row) => (Array.isArray(row) ? row : []))
      .map((row) =>
        row
          .map((btn) => {
            if (!btn || typeof btn !== 'object') return null;
            const anyBtn = btn as any;
            const text = typeof anyBtn.text === 'string' ? anyBtn.text.trim() : '';
            const url = typeof anyBtn.url === 'string' ? anyBtn.url.trim() : '';
            const callbackData =
              typeof anyBtn.callback_data === 'string'
                ? anyBtn.callback_data.trim()
                : typeof anyBtn.callbackData === 'string'
                  ? anyBtn.callbackData.trim()
                  : '';
            if (!text) return null;
            if (url) return { ...anyBtn, text, url };
            if (callbackData) return { ...anyBtn, text, callback_data: callbackData };
            return null;
          })
          .filter(Boolean),
      )
      .filter((row) => row.length > 0);
  };

  // UI/DB may store buttons as 2D array directly
  const rowsFromArray = toKeyboardRows(input);
  if (rowsFromArray) {
    const normalized = normalizeRows(rowsFromArray);
    return normalized.length ? { inline_keyboard: normalized } : undefined;
  }

  // Or store full markup object
  if (typeof input === 'object') {
    const obj = input as any;
    if ('inline_keyboard' in obj) {
      const rows = toKeyboardRows(obj.inline_keyboard);
      if (rows) {
        const normalized = normalizeRows(rows);
        return normalized.length ? { ...obj, inline_keyboard: normalized } : undefined;
      }
    }
  }

  return input;
}

async function finalizeCampaignIfDone(args: {
  campaignId: bigint;
  scheduleType: string | null | undefined;
}) {
  if (args.scheduleType === 'recurring') return;

  const campaign = await prisma.massMessageCampaign.findUnique({
    where: { id: args.campaignId },
    select: {
      id: true,
      progressSent: true,
      progressTotal: true,
      progressFailed: true,
      status: true,
    },
  });

  if (!campaign) return;
  if (campaign.progressTotal <= 0) return;
  if (campaign.progressSent < campaign.progressTotal) return;

  const finalStatus = campaign.progressFailed > 0 ? 'failed' : 'completed';
  if (campaign.status === (finalStatus as any)) return;

  await prisma.massMessageCampaign.update({
    where: { id: args.campaignId },
    data: { status: finalStatus as any },
  });
}

export async function handleMassMessageItem(itemIdRaw: string) {
  const itemId = BigInt(itemIdRaw);
  const now = new Date();

  const item = await prisma.massMessageItem.findUnique({
    where: { id: itemId },
    include: {
      campaign: true,
    },
  });

  if (!item) {
    throw new Error(`未找到群发条目: ${itemIdRaw}`);
  }

  // mark running (best-effort)
  await prisma.massMessageItem.update({
    where: { id: itemId },
    data: { status: 'running' as any, startedAt: now },
  });

  try {
    const campaign = item.campaign;

    const template = campaign.templateId
      ? await prisma.messageTemplate.findUnique({
          where: { id: campaign.templateId },
        })
      : null;

    const text =
      campaign.contentOverride ??
      template?.content ??
      (() => {
        throw new Error('群发活动缺少内容（template/contentOverride）');
      })();

    const format =
      (campaign.formatOverride as any) ??
      (template?.format as any) ??
      'html';

    const replyMarkup =
      normalizeReplyMarkup(campaign.buttonsOverride ?? template?.buttons ?? undefined);

    const mediaUrl = (campaign.imageUrlOverride ?? template?.imageUrl ?? '').trim();
    const isVideo = mediaUrl
      ? /\.(mp4|webm|ogg|mov|m4v|avi|mkv)(\?.*)?$/i.test(mediaUrl)
      : false;

    if (item.targetType !== 'channel') {
      // v1: group pin not supported; sending to group may also be unsupported depending on targetId semantics
      throw new Error(`不支持的 targetType=${item.targetType}（v1 仅支持频道）`);
    }

    // targetId is channelId (BigInt as string) in v1
    const channel = await prisma.channel.findUnique({
      where: { id: BigInt(item.targetId) },
      select: {
        id: true,
        tgChatId: true,
        defaultBotId: true,
        status: true,
        adMessageId: true,
        adPinEnabled: true,
      },
    });

    if (!channel) {
      throw new Error(`未找到目标频道: ${item.targetId}`);
    }
    if (channel.status !== 'active') {
      throw new Error(`频道未启用: ${channel.status}`);
    }
    if (!channel.defaultBotId) {
      throw new Error('频道未配置默认机器人');
    }

    const bot = await prisma.bot.findUnique({
      where: { id: channel.defaultBotId },
      select: { id: true, status: true, tokenEncrypted: true },
    });
    if (!bot) throw new Error(`未找到机器人: ${channel.defaultBotId.toString()}`);
    if (bot.status !== 'active') throw new Error(`机器人未启用: ${bot.status}`);

    const parseMode = resolveParseMode(format);

    const sendResult = mediaUrl
      ? isVideo
        ? await sendVideoByTelegram({
            botToken: bot.tokenEncrypted,
            chatId: channel.tgChatId,
            fileId: mediaUrl,
            caption: text,
            parseMode,
            replyMarkup,
          })
        : await sendPhotoByTelegram({
            botToken: bot.tokenEncrypted,
            chatId: channel.tgChatId,
            fileId: mediaUrl,
            caption: text,
            parseMode,
            replyMarkup,
          })
      : await sendTextByTelegram({
          botToken: bot.tokenEncrypted,
          chatId: channel.tgChatId,
          text,
          parseMode,
          replyMarkup,
          disableWebPagePreview: false,
        });

    let pinSuccess: boolean | null = null;
    let pinErrorMessage: string | null = null;

    const pinMode = campaign.pinMode as unknown as 'none' | 'pin_after_send' | 'replace_pin';
    const shouldPin = pinMode !== 'none';

    if (shouldPin) {
      try {
        if (pinMode === 'replace_pin' && channel.adMessageId) {
          // Important: only unpin bot-maintained previous ad message
          await unpinMessageByTelegram({
            botToken: bot.tokenEncrypted,
            chatId: channel.tgChatId,
            messageId: Number(channel.adMessageId),
          });
        }

        await pinMessageByTelegram({
          botToken: bot.tokenEncrypted,
          chatId: channel.tgChatId,
          messageId: sendResult.messageId,
        });

        pinSuccess = true;
      } catch (pinErr) {
        pinSuccess = false;
        pinErrorMessage = pinErr instanceof Error ? pinErr.message : '置顶失败';
      }
    }

    await prisma.$transaction([
      prisma.massMessageItem.update({
        where: { id: itemId },
        data: {
          status: 'success' as any,
          finishedAt: new Date(),
          telegramMessageId: BigInt(sendResult.messageId),
          telegramMessageLink: sendResult.messageLink,
          telegramErrorCode: null,
          telegramErrorMessage: null,
          pinSuccess,
          pinErrorMessage,
        },
      }),
      prisma.massMessageCampaign.update({
        where: { id: campaign.id },
        data: {
          status: 'running' as any,
          progressSent: { increment: 1 },
          progressSuccess: { increment: 1 },
          lastError: null,
        },
      }),
      // track bot-maintained pinned ad message id when pin is enabled
      ...(shouldPin
        ? [
            prisma.channel.update({
              where: { id: channel.id },
              data: {
                adMessageId: BigInt(sendResult.messageId),
                lastAdUpdateAt: new Date(),
              },
            }),
          ]
        : []),
    ]);

    await finalizeCampaignIfDone({
      campaignId: campaign.id,
      scheduleType: (campaign.scheduleType as any) ?? null,
    });

    // recurring: push next_run_at forward (v1: simple interval seconds if provided)
    if (campaign.scheduleType === 'recurring') {
      const intervalSec =
        typeof campaign.recurringPattern === 'object' &&
        campaign.recurringPattern !== null &&
        'intervalSec' in (campaign.recurringPattern as any)
          ? Number((campaign.recurringPattern as any).intervalSec)
          : null;

      if (intervalSec && intervalSec > 0) {
        await prisma.massMessageItem.update({
          where: { id: itemId },
          data: {
            status: 'pending' as any,
            retryCount: 0,
            nextRunAt: new Date(Date.now() + intervalSec * 1000),
            plannedAt: new Date(Date.now() + intervalSec * 1000),
          },
        });
      }
    }

    logger.info('[q_mass_message] 已发送群发消息', {
      itemId: itemIdRaw,
      campaignId: campaign.id.toString(),
      messageId: sendResult.messageId,
    });

    return { ok: true, itemId: itemIdRaw, messageId: sendResult.messageId };
  } catch (error) {
    const err = error as TelegramError;
    const message = (err as any)?.message ? String((err as any).message) : '未知错误';
    const code = (err as any)?.code ? String((err as any).code) : 'MASS_MESSAGE_ERROR';
    const retryAfterSec = (err as any)?.retryAfterSec as number | undefined;

    const nextRetryCount = item.retryCount + 1;
    const exceeded = nextRetryCount > item.maxRetries;
    const backoffSec = retryAfterSec ?? getBackoffSeconds(nextRetryCount);
    const nextRunAt = new Date(Date.now() + backoffSec * 1000);

    const nextStatus = exceeded ? 'dead' : 'failed';

    await prisma.$transaction([
      prisma.massMessageItem.update({
        where: { id: itemId },
        data: {
          status: nextStatus as any,
          retryCount: nextRetryCount,
          nextRunAt: exceeded ? item.nextRunAt : nextRunAt,
          telegramErrorCode: code,
          telegramErrorMessage: message,
          finishedAt: new Date(),
        },
      }),
      prisma.massMessageCampaign.update({
        where: { id: item.campaignId },
        data: {
          status: 'running' as any,
          progressSent: { increment: 1 },
          progressFailed: { increment: 1 },
          lastError: message,
        },
      }),
    ]);

    await finalizeCampaignIfDone({
      campaignId: item.campaignId,
      scheduleType: (item.campaign as any)?.scheduleType ?? null,
    });

    throw error;
  }
}


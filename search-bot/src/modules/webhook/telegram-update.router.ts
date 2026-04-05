import { parseSearchCommand } from '../command/command.parser';
import { renderSearchMessage } from '../render/message.renderer';
import { createCallbackToken } from '../callback/callback-token.service';
import { handleCallbackQuery } from '../callback/callback-handler';
import { allowChannelRequest, allowUserRequest } from '../security/rate-limit.service';
import { searchWithCache } from '../search/search.orchestrator';
import { logger } from '../../infra/logger';
import { renderResultKeyboard } from '../render/keyboard.renderer';
import { buildDeepLink, createDeepLinkToken } from '../deeplink/deeplink.service';
import { handleStartCommand } from '../start/start-handler';

export interface TelegramMessage {
  message_id?: number;
  text?: string;
  chat?: {
    id?: number;
    type?: string;
  };
  from?: {
    id?: number;
  };
  sender_chat?: {
    id?: number;
    type?: string;
  };
}

export interface TelegramUpdate {
  update_id?: number;
  message?: TelegramMessage;
  channel_post?: TelegramMessage;
  callback_query?: Record<string, unknown>;
}

export async function routeTelegramUpdate(update: TelegramUpdate) {
  if (update.message) {
    const text = update.message.text || '';
    if (text.startsWith('/start')) {
      const chatId = update.message.chat?.id;
      if (typeof chatId === 'number') {
        return handleStartCommand({
          chatId,
          fromId: update.message.from?.id,
          text,
          routed: 'message',
        });
      }
    }
    return handleTextMessage(update.message, 'message');
  }

  if (update.channel_post) {
    const text = update.channel_post.text || '';
    if (text.startsWith('/start')) {
      const chatId = update.channel_post.chat?.id;
      if (typeof chatId === 'number') {
        return handleStartCommand({
          chatId,
          fromId: update.channel_post.from?.id,
          text,
          routed: 'channel_post',
        });
      }
    }

    return handleTextMessage(update.channel_post, 'channel_post');
  }

  if (update.callback_query) {
    return handleCallbackQuery(update.callback_query);
  }

  return { routed: 'ignored', ok: true };
}

async function handleTextMessage(message: TelegramMessage, routed: 'message' | 'channel_post') {
  logger.info('update.text_received', {
    routed,
    text: message.text,
    chatId: message.chat?.id,
    fromId: message.from?.id,
    senderChatId: message.sender_chat?.id,
  });

  const parsed = parseSearchCommand(message.text);
  if (!parsed) {
    logger.info('update.command_ignored', {
      routed,
      reason: 'parse_failed_or_not_search_command',
      text: message.text,
    });
    return { routed, ok: true, action: 'noop' };
  }

  const chatId = message.chat?.id;
  const channelId = typeof chatId === 'number' ? String(chatId) : '';
  if (!channelId) {
    logger.warn('update.invalid_context', {
      routed,
      reason: 'missing_chat_id',
    });
    return { routed, ok: false, action: 'invalid_context' };
  }

  const requesterIdRaw = message.from?.id;
  const requesterId = typeof requesterIdRaw === 'number' ? String(requesterIdRaw) : '*';

  if (requesterId !== '*') {
    const allowedByUser = await allowUserRequest(requesterId);
    if (!allowedByUser) {
      return {
        routed,
        ok: true,
        action: 'rate_limited_user',
        send: {
          chatId,
          text: '请求过于频繁，请稍后再试。',
        },
      };
    }
  }

  const allowedByChannel = await allowChannelRequest(channelId);
  if (!allowedByChannel) {
    return {
      routed,
      ok: true,
      action: 'rate_limited_channel',
      send: {
        chatId,
        text: '当前频道搜索请求过多，请稍后重试。',
      },
    };
  }

  const page = 1;
  const pageSize = 20;
  const result = await searchWithCache({
    keyword: parsed.keyword,
    channelId,
    limit: pageSize,
    offset: 0,
  });

  const nextToken = result.hasMore
    ? await createCallbackToken({
        keyword: parsed.keyword,
        channelId,
        requesterId,
        page: page + 1,
        pageSize,
        mode: 'page',
      })
    : null;

  const renderItems: Array<{ title?: string; year?: number | null; actors?: string[]; deepLink?: string }> = [];
  const copyButtons: Array<{ text: string; token: string }> = [];

  for (let index = 0; index < Math.min(result.results.length, pageSize); index += 1) {
    const item = result.results[index] as Record<string, unknown>;
    const fromChatId = String(item.channelTgChatId ?? item.channel_tg_chat_id ?? '');
    const messageIdRaw = item.telegramMessageId ?? item.telegram_message_id ?? 0;
    const messageId = Number(messageIdRaw);

    if (fromChatId && Number.isFinite(messageId) && messageId > 0) {
      const shortToken = await createDeepLinkToken({
        fromChatId,
        messageId,
        targetChatId: channelId,
        requesterId: requesterId === '*' ? undefined : requesterId,
        title: String(item.title ?? ''),
        telegramMessageLink: String(item.telegramMessageLink ?? ''),
      });

      renderItems.push({
        title: String(item.title ?? ''),
        year: typeof item.year === 'number' ? item.year : null,
        actors: Array.isArray(item.actors) ? (item.actors as string[]) : [],
        deepLink: buildDeepLink(shortToken),
      });

      const copyToken = await createCallbackToken({
        keyword: parsed.keyword,
        channelId,
        requesterId,
        page,
        pageSize,
        mode: 'copy',
        docId: String(item.docId ?? ''),
        fromChatId,
        messageId,
        targetChatId: channelId,
        item: {
          title: item.title,
          telegramMessageLink: item.telegramMessageLink,
        },
      });

      copyButtons.push({
        text: `发送 ${String(index + 1).padStart(2, '0')}`,
        token: copyToken,
      });
    } else {
      renderItems.push({
        title: String(item.title ?? ''),
        year: typeof item.year === 'number' ? item.year : null,
        actors: Array.isArray(item.actors) ? (item.actors as string[]) : [],
      });
    }
  }

  const text = renderSearchMessage({
    keyword: parsed.keyword,
    page,
    pageSize,
    total: result.total,
    items: renderItems,
  });

  return {
    routed,
    ok: true,
    action: 'send_message',
    send: {
      chatId,
      text,
      parseMode: 'HTML',
      replyMarkup: renderResultKeyboard({
        copyButtons,
        prevToken: null,
        nextToken,
      }),
      degraded: result.total === 0,
    },
  };
}

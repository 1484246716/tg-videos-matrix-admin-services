import { parseSearchCommand } from '../command/command.parser';
import { logger } from '../../infra/logger';
import { renderSearchMessage } from '../render/message.renderer';
import { renderPagerKeyboard } from '../render/keyboard.renderer';
import { createCallbackToken } from '../callback/callback-token.service';
import { handleCallbackQuery } from '../callback/callback-handler';
import { allowChannelRequest, allowUserRequest } from '../security/rate-limit.service';
import { searchWithCache } from '../search/search.orchestrator';

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
    return handleTextMessage(update.message, 'message');
  }

  if (update.channel_post) {
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

  // 频道帖子可能没有 from，使用通配符允许 callback 由任意点击者翻页
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
      })
    : null;

  const text = renderSearchMessage({
    keyword: parsed.keyword,
    page,
    pageSize,
    total: result.total,
    items: result.results,
  });

  return {
    routed,
    ok: true,
    action: 'send_message',
    send: {
      chatId,
      text,
      replyMarkup: renderPagerKeyboard({ prevToken: null, nextToken }),
      degraded: result.total === 0,
    },
  };
}

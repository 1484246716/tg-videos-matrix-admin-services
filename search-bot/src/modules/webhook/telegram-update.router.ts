import { parseSearchCommand } from '../command/command.parser';
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
}

export interface TelegramUpdate {
  update_id?: number;
  message?: TelegramMessage;
  callback_query?: Record<string, unknown>;
}

export async function routeTelegramUpdate(update: TelegramUpdate) {
  if (update.message) {
    return handleMessage(update.message);
  }

  if (update.callback_query) {
    return handleCallbackQuery(update.callback_query);
  }

  return { routed: 'ignored', ok: true };
}

async function handleMessage(message: TelegramMessage) {
  const parsed = parseSearchCommand(message.text);
  if (!parsed) {
    return { routed: 'message', ok: true, action: 'noop' };
  }

  const chatId = message.chat?.id;
  const requesterId = message.from?.id;
  const channelId = typeof chatId === 'number' ? String(chatId) : '';

  if (!channelId || typeof requesterId !== 'number') {
    return { routed: 'message', ok: false, action: 'invalid_context' };
  }

  const allowedByUser = await allowUserRequest(String(requesterId));
  if (!allowedByUser) {
    return {
      routed: 'message',
      ok: true,
      action: 'rate_limited_user',
      send: {
        chatId,
        text: '请求过于频繁，请稍后再试。',
      },
    };
  }

  const allowedByChannel = await allowChannelRequest(channelId);
  if (!allowedByChannel) {
    return {
      routed: 'message',
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
        requesterId: String(requesterId),
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
    routed: 'message',
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

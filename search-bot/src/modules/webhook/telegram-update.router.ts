import { parseSearchCommand } from '../command/command.parser';
import { querySearch } from '../search/search.client';

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
    return { routed: 'callback_query', ok: true };
  }

  return { routed: 'ignored', ok: true };
}

async function handleMessage(message: TelegramMessage) {
  const parsed = parseSearchCommand(message.text);
  if (!parsed) {
    return { routed: 'message', ok: true, action: 'noop' };
  }

  const chatId = message.chat?.id;
  const channelIds = typeof chatId === 'number' ? [String(chatId)] : [];

  const result = await querySearch({
    keyword: parsed.keyword,
    channelIds,
    limit: 20,
    offset: 0,
    fallbackToDb: true,
  });

  return {
    routed: 'message',
    ok: true,
    action: 'search',
    keyword: parsed.keyword,
    total: result.total,
    hasMore: result.hasMore,
    route: result.route,
  };
}

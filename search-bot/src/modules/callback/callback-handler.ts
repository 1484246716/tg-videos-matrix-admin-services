import { readCallbackToken, createCallbackToken } from './callback-token.service';
import { renderSearchMessage } from '../render/message.renderer';
import { renderPagerKeyboard } from '../render/keyboard.renderer';
import { allowChannelRequest, allowUserRequest } from '../security/rate-limit.service';
import { searchWithCache } from '../search/search.orchestrator';

export interface TelegramCallbackQuery {
  id?: string;
  data?: string;
  from?: {
    id?: number;
  };
  message?: {
    message_id?: number;
    chat?: {
      id?: number;
    };
  };
}

export async function handleCallbackQuery(callback: TelegramCallbackQuery) {
  const data = callback.data;
  if (!data || !data.startsWith('sp:')) {
    return { routed: 'callback_query', ok: true, action: 'noop' };
  }

  const token = data.slice(3);
  const state = await readCallbackToken(token);
  if (!state) {
    return { routed: 'callback_query', ok: true, action: 'expired' };
  }

  const requesterId = String(callback.from?.id ?? '');
  if (!requesterId || requesterId !== state.requesterId) {
    return { routed: 'callback_query', ok: true, action: 'forbidden' };
  }

  const allowedByUser = await allowUserRequest(requesterId);
  if (!allowedByUser) {
    return { routed: 'callback_query', ok: true, action: 'rate_limited_user' };
  }

  const allowedByChannel = await allowChannelRequest(state.channelId);
  if (!allowedByChannel) {
    return { routed: 'callback_query', ok: true, action: 'rate_limited_channel' };
  }

  const result = await searchWithCache({
    keyword: state.keyword,
    channelId: state.channelId,
    limit: state.pageSize,
    offset: (state.page - 1) * state.pageSize,
  });

  const hasPrev = state.page > 1;
  const hasNext = state.page * state.pageSize < result.total;

  const prevToken = hasPrev
    ? await createCallbackToken({
        ...state,
        page: state.page - 1,
      })
    : null;

  const nextToken = hasNext
    ? await createCallbackToken({
        ...state,
        page: state.page + 1,
      })
    : null;

  const text = renderSearchMessage({
    keyword: state.keyword,
    page: state.page,
    pageSize: state.pageSize,
    total: result.total,
    items: result.results,
  });

  return {
    routed: 'callback_query',
    ok: true,
    action: 'edit_message',
    edit: {
      chatId: callback.message?.chat?.id,
      messageId: callback.message?.message_id,
      text,
      replyMarkup: renderPagerKeyboard({ prevToken, nextToken }),
      degraded: result.total === 0,
    },
  };
}

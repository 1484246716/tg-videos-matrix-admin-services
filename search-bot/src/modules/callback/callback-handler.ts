import axios from 'axios';
import { readCallbackToken, createCallbackToken } from './callback-token.service';
import { renderSearchMessage } from '../render/message.renderer';
import { allowUserRequest } from '../security/rate-limit.service';
import { searchWithCache } from '../search/search.orchestrator';
import { renderResultKeyboard } from '../render/keyboard.renderer';
import { copyMessage, forwardMessage, getChatMember, getMe } from '../telegram/telegram.client';
import { env } from '../../config/env';
import { setIfAbsent } from '../../infra/redis';

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

function isRetryableError(error: unknown): boolean {
  if (!axios.isAxiosError(error)) return false;
  const status = error.response?.status;
  return status === 429 || (typeof status === 'number' && status >= 500);
}

function getRetryAfterMs(error: unknown): number {
  if (!axios.isAxiosError(error)) return env.SEARCH_BOT_COPY_RETRY_BACKOFF_MS;
  const retryAfter = Number(
    (error.response?.data as { parameters?: { retry_after?: number } } | undefined)?.parameters?.retry_after,
  );
  if (Number.isFinite(retryAfter) && retryAfter > 0) return retryAfter * 1000;
  return env.SEARCH_BOT_COPY_RETRY_BACKOFF_MS;
}

async function withRetry<T>(fn: () => Promise<T>): Promise<T> {
  let attempt = 0;
  let lastError: unknown;

  while (attempt <= env.SEARCH_BOT_COPY_RETRY_MAX) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (!isRetryableError(error) || attempt >= env.SEARCH_BOT_COPY_RETRY_MAX) {
        break;
      }
      const backoff = getRetryAfterMs(error) * (attempt + 1);
      await new Promise((resolve) => setTimeout(resolve, backoff));
      attempt += 1;
    }
  }

  throw lastError;
}

async function ensureBotPermission(chatId: string): Promise<boolean> {
  const me = await getMe();
  if (!me?.id) return false;
  const member = await getChatMember({ chatId, userId: me.id });
  if (!member?.status) return false;
  if (member.status === 'administrator' || member.status === 'creator') return true;
  return Boolean(member.can_post_messages);
}

export async function handleCallbackQuery(callback: TelegramCallbackQuery) {
  const data = callback.data;
  if (!data) {
    return { routed: 'callback_query', ok: true, action: 'noop' };
  }

  if (data.startsWith('sc:')) {
    const token = data.slice(3);
    const state = await readCallbackToken(token);
    if (!state || state.mode !== 'copy') {
      return { routed: 'callback_query', ok: true, action: 'expired' };
    }

    const requesterId = String(callback.from?.id ?? '');
    const allowAnyRequester = state.requesterId === '*';
    if (!allowAnyRequester && (!requesterId || requesterId !== state.requesterId)) {
      return { routed: 'callback_query', ok: true, action: 'forbidden' };
    }

    const targetChatId = state.targetChatId || String(callback.message?.chat?.id ?? '');
    const fromChatId = state.fromChatId || '';
    const messageId = Number(state.messageId ?? 0);

    if (!targetChatId || !fromChatId || !Number.isFinite(messageId) || messageId <= 0) {
      return { routed: 'callback_query', ok: true, action: 'copy_invalid_payload' };
    }

    const idemKey = `sb:copy:idem:${fromChatId}:${messageId}:${targetChatId}`;
    const firstCopy = await setIfAbsent(idemKey, '1', env.SEARCH_BOT_COPY_IDEMPOTENT_TTL_SEC);
    if (!firstCopy) {
      return { routed: 'callback_query', ok: true, action: 'copy_duplicate' };
    }

    const [sourceAllowed, targetAllowed] = await Promise.all([
      ensureBotPermission(fromChatId),
      ensureBotPermission(targetChatId),
    ]);

    if (!sourceAllowed || !targetAllowed) {
      return { routed: 'callback_query', ok: true, action: 'copy_permission_denied' };
    }

    try {
      await withRetry(() => copyMessage({ chatId: targetChatId, fromChatId, messageId }));
      return { routed: 'callback_query', ok: true, action: 'copy_success' };
    } catch {
      try {
        await withRetry(() => forwardMessage({ chatId: targetChatId, fromChatId, messageId }));
        return { routed: 'callback_query', ok: true, action: 'copy_fallback_forward' };
      } catch {
        const link = String(state.item?.telegramMessageLink ?? '');
        if (link) {
          return {
            routed: 'callback_query',
            ok: true,
            action: 'send_message',
            send: {
              chatId: callback.message?.chat?.id,
              text: `发送失败，可直接查看原消息：${link}`,
            },
          };
        }
        return { routed: 'callback_query', ok: true, action: 'copy_failed' };
      }
    }
  }

  if (!data.startsWith('sp:')) {
    return { routed: 'callback_query', ok: true, action: 'noop' };
  }

  const token = data.slice(3);
  const state = await readCallbackToken(token);
  if (!state) {
    return { routed: 'callback_query', ok: true, action: 'expired' };
  }

  const requesterId = String(callback.from?.id ?? '');
  const allowAnyRequester = state.requesterId === '*';
  if (!allowAnyRequester && (!requesterId || requesterId !== state.requesterId)) {
    return { routed: 'callback_query', ok: true, action: 'forbidden' };
  }

  if (!allowAnyRequester) {
    const allowedByUser = await allowUserRequest(requesterId);
    if (!allowedByUser) {
      return { routed: 'callback_query', ok: true, action: 'rate_limited_user' };
    }
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
        mode: 'page',
      })
    : null;

  const nextToken = hasNext
    ? await createCallbackToken({
        ...state,
        page: state.page + 1,
        mode: 'page',
      })
    : null;

  const copyButtons: Array<{ text: string; token: string }> = [];

  const pageItems = result.results.slice(0, state.pageSize);
  for (let index = 0; index < pageItems.length; index += 1) {
    const item = pageItems[index] as Record<string, unknown>;
    const row = item as Record<string, unknown>;
    const fromChatId = String(row.channelTgChatId ?? row.channel_tg_chat_id ?? '');
    const messageId = Number(row.telegramMessageId ?? row.telegram_message_id ?? 0);

    if (!fromChatId || !Number.isFinite(messageId) || messageId <= 0) {
      continue;
    }

    const token = await createCallbackToken({
      keyword: state.keyword,
      channelId: state.channelId,
      requesterId: state.requesterId,
      page: state.page,
      pageSize: state.pageSize,
      mode: 'copy',
      docId: String(item.docId ?? ''),
      fromChatId,
      messageId,
      targetChatId: String(callback.message?.chat?.id ?? ''),
      item: {
        title: item.title,
        telegramMessageLink: item.telegramMessageLink,
      },
    });

    copyButtons.push({
      text: `${String(index + 1).padStart(2, '0')}. ${String(item.title ?? '发送该条')}`,
      token,
    });
  }

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
      parseMode: 'HTML',
      replyMarkup: renderResultKeyboard({
        copyButtons,
        prevToken,
        nextToken,
      }),
      degraded: result.total === 0,
    },
  };
}

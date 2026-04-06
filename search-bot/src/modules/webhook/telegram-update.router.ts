import { parseSearchCommand } from '../command/command.parser';
import { renderSearchMessage } from '../render/message.renderer';
import { handleCallbackQuery } from '../callback/callback-handler';
import { allowUserRequest } from '../security/rate-limit.service';
import { searchWithCache } from '../search/search.orchestrator';
import { logger } from '../../infra/logger';
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
}

export interface TelegramUpdate {
  update_id?: number;
  message?: TelegramMessage;
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
          triggerMessageId: update.message.message_id,
        });
      }
    }

    return handleTextMessage(update.message);
  }

  if (update.callback_query) {
    return handleCallbackQuery(update.callback_query);
  }

  return { routed: 'ignored', ok: true };
}

function isCollectionLikeTitle(title: string): boolean {
  const trimmed = title.trim();
  if (!trimmed) return false;

  // 常见“合集项”模式：关键词后直接跟纯数字，如 黄宏1 / 黄宏10
  if (/^[\u4e00-\u9fa5A-Za-z]+\d{1,3}$/.test(trimmed)) {
    return true;
  }

  // 显式合集关键词
  if (/(合集|全集|全季|系列)/.test(trimmed)) {
    return true;
  }

  return false;
}

async function handleTextMessage(message: TelegramMessage) {
  logger.info('收到私聊文本消息', {
    text: message.text,
    chatId: message.chat?.id,
    fromId: message.from?.id,
  });

  const parsed = parseSearchCommand(message.text);
  if (!parsed) {
    logger.info('忽略非搜索命令文本', {
      reason: 'parse_failed_or_not_search_command',
      text: message.text,
    });
    return { routed: 'message', ok: true, action: 'noop' };
  }

  const chatId = message.chat?.id;
  const channelId = typeof chatId === 'number' ? String(chatId) : '';
  if (!channelId) {
    logger.warn('私聊上下文无chatId', {
      reason: 'missing_chat_id',
    });
    return { routed: 'message', ok: false, action: 'invalid_context' };
  }

  const requesterIdRaw = message.from?.id;
  const requesterId = typeof requesterIdRaw === 'number' ? String(requesterIdRaw) : '*';

  if (requesterId !== '*') {
    const allowedByUser = await allowUserRequest(requesterId);
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
  }

  const page = 1;
  const pageSize = 20;
  const result = await searchWithCache({
    keyword: parsed.keyword,
    channelId,
    limit: pageSize,
    offset: 0,
  });

  const renderItems: Array<{ title?: string; year?: number | null; actors?: string[]; deepLink?: string }> = [];

  const pageRows = result.results
    .filter((row) => !isCollectionLikeTitle(String((row as Record<string, unknown>).title ?? '')))
    .slice(0, pageSize);

  if (result.results.length > pageRows.length) {
    logger.info('搜索结果已过滤合集标题', {
      keyword: parsed.keyword,
      originalCount: result.results.length,
      filteredCount: pageRows.length,
      removedCount: result.results.length - pageRows.length,
    });
  }

  for (let index = 0; index < pageRows.length; index += 1) {
    const item = pageRows[index] as Record<string, unknown>;
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


    } else {
      const fallbackLink = String(item.telegramMessageLink ?? item.telegram_message_link ?? '');
      renderItems.push({
        title: String(item.title ?? ''),
        year: typeof item.year === 'number' ? item.year : null,
        actors: Array.isArray(item.actors) ? (item.actors as string[]) : [],
        deepLink: fallbackLink || undefined,
      });
    }
  }

  const text = renderSearchMessage({
    keyword: parsed.keyword,
    page,
    pageSize,
    total: pageRows.length,
    items: renderItems,
  });

  return {
    routed: 'message',
    ok: true,
    action: 'send_message',
    send: {
      chatId,
      text,
      parseMode: 'HTML',
      degraded: result.total === 0,
    },
  };
}

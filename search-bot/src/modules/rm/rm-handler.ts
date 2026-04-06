import { createCallbackToken } from '../callback/callback-token.service';
import { renderHotMessage } from '../render/rm-message.renderer';
import { renderHotKeyboard } from '../render/tags-keyboard.renderer';
import { queryHot } from '../search/search.client';
import { logger } from '../../infra/logger';
import { buildDeepLink, createDeepLinkToken } from '../deeplink/deeplink.service';

const PAGE_SIZE = 20;

export async function handleRmCommand(args: { channelId: string; requesterId: string; page?: number }) {
  const page = Math.max(1, args.page || 1);
  const offset = (page - 1) * PAGE_SIZE;
  const startedAt = Date.now();

  const result = await queryHot({
    channelIds: [args.channelId],
    limit: PAGE_SIZE,
    offset,
    period: '7d',
    fallbackToDb: true,
  });

  logger.info('rm.query.completed', {
    channelId: args.channelId,
    requesterId: args.requesterId,
    page,
    pageSize: PAGE_SIZE,
    total: result.total,
    route: result.route,
    durationMs: Date.now() - startedAt,
  });

  const hasPrev = page > 1;
  const hasNext = page * PAGE_SIZE < result.total;

  const prevToken = hasPrev
    ? await createCallbackToken({
        channelId: args.channelId,
        requesterId: args.requesterId,
        page: page - 1,
        pageSize: PAGE_SIZE,
        mode: 'rm_page',
      })
    : null;

  const nextToken = hasNext
    ? await createCallbackToken({
        channelId: args.channelId,
        requesterId: args.requesterId,
        page: page + 1,
        pageSize: PAGE_SIZE,
        mode: 'rm_page',
      })
    : null;

  const pageRows = result.results.slice(0, PAGE_SIZE);

  const renderItems: Array<{ title?: string; year?: number | null; actors?: string[]; deepLink?: string }> = [];

  for (const item of pageRows) {
    const row = item as Record<string, unknown>;
    const fromChatId = String(row.channelTgChatId ?? row.channel_tg_chat_id ?? '');
    const messageId = Number(row.telegramMessageId ?? row.telegram_message_id ?? 0);

    let deepLink: string | undefined;
    if (fromChatId && Number.isFinite(messageId) && messageId > 0) {
      const shortToken = await createDeepLinkToken({
        fromChatId,
        messageId,
        targetChatId: args.channelId,
        requesterId: args.requesterId === '*' ? undefined : args.requesterId,
        title: String(row.title ?? ''),
        telegramMessageLink: String(row.telegramMessageLink ?? row.telegram_message_link ?? ''),
      });
      deepLink = buildDeepLink(shortToken);
    } else {
      const fallbackLink = String(row.telegramMessageLink ?? row.telegram_message_link ?? '');
      deepLink = fallbackLink || undefined;
    }

    renderItems.push({
      title: String(row.title ?? ''),
      year: typeof row.year === 'number' ? row.year : null,
      actors: Array.isArray(row.actors) ? (row.actors as string[]) : [],
      deepLink,
    });
  }

  const text = renderHotMessage({
    page,
    pageSize: PAGE_SIZE,
    total: result.total,
    period: '7d',
    items: renderItems,
  });

  return {
    ok: true as const,
    action: 'send_message' as const,
    send: {
      text,
      parseMode: 'HTML' as const,
      replyMarkup: renderHotKeyboard({ prevToken, nextToken }),
    },
  };
}

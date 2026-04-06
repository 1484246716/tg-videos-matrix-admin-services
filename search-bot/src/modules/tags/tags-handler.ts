import { createCallbackToken } from '../callback/callback-token.service';
import { renderTagResultKeyboard, renderTagsKeyboard } from '../render/tags-keyboard.renderer';
import { renderTagResultMessage, renderTagsPanelMessage } from '../render/tags-message.renderer';
import { queryByTag, queryLevel2Tags, queryTags } from '../search/search.client';
import { logger } from '../../infra/logger';
import { buildDeepLink, createDeepLinkToken } from '../deeplink/deeplink.service';

const TAGS_PAGE_SIZE = 24;
const RESULT_PAGE_SIZE = 20;

export async function handleTagsCommand(args: { channelId: string; requesterId: string; page?: number }) {
  const page = Math.max(1, args.page || 1);
  const offset = (page - 1) * TAGS_PAGE_SIZE;
  const startedAt = Date.now();

  const result = await queryTags({
    channelIds: [args.channelId],
    limit: TAGS_PAGE_SIZE,
    offset,
  });

  logger.info('tags.level1.query.completed', {
    channelId: args.channelId,
    requesterId: args.requesterId,
    page,
    pageSize: TAGS_PAGE_SIZE,
    total: result.total,
    route: result.route,
    durationMs: Date.now() - startedAt,
  });

  const hasPrev = page > 1;
  const hasNext = page * TAGS_PAGE_SIZE < result.total;

  const prevToken = hasPrev
    ? await createCallbackToken({
        channelId: args.channelId,
        requesterId: args.requesterId,
        page: page - 1,
        pageSize: TAGS_PAGE_SIZE,
        mode: 'tag_menu',
      })
    : null;

  const nextToken = hasNext
    ? await createCallbackToken({
        channelId: args.channelId,
        requesterId: args.requesterId,
        page: page + 1,
        pageSize: TAGS_PAGE_SIZE,
        mode: 'tag_menu',
      })
    : null;

  const selectTokenByTagId = new Map<string, string>();
  for (const tag of result.tags) {
    const token = await createCallbackToken({
      channelId: args.channelId,
      requesterId: args.requesterId,
      page: 1,
      pageSize: TAGS_PAGE_SIZE,
      mode: 'tag_level2',
      level1Id: tag.id,
      level1Name: tag.name,
    });
    selectTokenByTagId.set(tag.id, token);
  }

  return {
    ok: true as const,
    action: 'send_message' as const,
    send: {
      text: renderTagsPanelMessage({
        page,
        pageSize: TAGS_PAGE_SIZE,
        total: result.total,
        tags: result.tags,
      }),
      parseMode: 'HTML' as const,
      replyMarkup: renderTagsKeyboard({
        tags: result.tags,
        selectTokenByTagId,
        selectPrefix: 'tg:l1:',
        pagerPrefix: 'tg:m:',
        prevToken,
        nextToken,
      }),
    },
  };
}

export async function handleLevel2Tags(args: {
  channelId: string;
  requesterId: string;
  level1Id: string;
  level1Name?: string;
  page?: number;
}) {
  const page = Math.max(1, args.page || 1);
  const offset = (page - 1) * TAGS_PAGE_SIZE;
  const startedAt = Date.now();

  const result = await queryLevel2Tags({
    channelIds: [args.channelId],
    level1Id: args.level1Id,
    limit: TAGS_PAGE_SIZE,
    offset,
  });

  logger.info('tags.level2.query.completed', {
    channelId: args.channelId,
    requesterId: args.requesterId,
    level1Id: args.level1Id,
    level1Name: args.level1Name,
    page,
    pageSize: TAGS_PAGE_SIZE,
    total: result.total,
    route: result.route,
    durationMs: Date.now() - startedAt,
  });

  const hasPrev = page > 1;
  const hasNext = page * TAGS_PAGE_SIZE < result.total;

  const prevToken = hasPrev
    ? await createCallbackToken({
        channelId: args.channelId,
        requesterId: args.requesterId,
        page: page - 1,
        pageSize: TAGS_PAGE_SIZE,
        mode: 'tag_level2',
        level1Id: args.level1Id,
        level1Name: args.level1Name,
      })
    : null;

  const nextToken = hasNext
    ? await createCallbackToken({
        channelId: args.channelId,
        requesterId: args.requesterId,
        page: page + 1,
        pageSize: TAGS_PAGE_SIZE,
        mode: 'tag_level2',
        level1Id: args.level1Id,
        level1Name: args.level1Name,
      })
    : null;

  const selectTokenByTagId = new Map<string, string>();
  for (const tag of result.tags) {
    const token = await createCallbackToken({
      channelId: args.channelId,
      requesterId: args.requesterId,
      page: 1,
      pageSize: RESULT_PAGE_SIZE,
      mode: 'tag_result',
      tagId: tag.id,
      tagName: tag.name,
      level1Id: args.level1Id,
      level1Name: args.level1Name,
    });
    selectTokenByTagId.set(tag.id, token);
  }

  return {
    ok: true as const,
    action: 'send_message' as const,
    send: {
      text: [
        `<b>一级分类：${escapeHtml(args.level1Name || '未命名')}</b>`,
        `<b>第 ${page}/${Math.max(1, Math.ceil(result.total / TAGS_PAGE_SIZE))} 页｜共 ${result.total} 个二级分类</b>`,
        '请选择二级分类查看片单。',
      ].join('\n'),
      parseMode: 'HTML' as const,
      replyMarkup: renderTagsKeyboard({
        tags: result.tags,
        selectTokenByTagId,
        selectPrefix: 'tg:s:',
        pagerPrefix: 'tg:l2m:',
        prevToken,
        nextToken,
      }),
    },
  };
}

export async function handleTagResult(args: {
  channelId: string;
  requesterId: string;
  tagId?: string;
  tagName?: string;
  level1Id?: string;
  page?: number;
}) {
  const page = Math.max(1, args.page || 1);
  const offset = (page - 1) * RESULT_PAGE_SIZE;
  const startedAt = Date.now();

  const result = await queryByTag({
    channelIds: [args.channelId],
    tagId: args.tagId,
    tagName: args.tagName,
    limit: RESULT_PAGE_SIZE,
    offset,
    fallbackToDb: true,
  });

  logger.info('tags.result.query.completed', {
    channelId: args.channelId,
    requesterId: args.requesterId,
    tagId: args.tagId,
    tagName: args.tagName,
    level1Id: args.level1Id,
    page,
    pageSize: RESULT_PAGE_SIZE,
    total: result.total,
    route: result.route,
    durationMs: Date.now() - startedAt,
  });

  const hasPrev = page > 1;
  const hasNext = page * RESULT_PAGE_SIZE < result.total;

  const prevToken = hasPrev
    ? await createCallbackToken({
        channelId: args.channelId,
        requesterId: args.requesterId,
        page: page - 1,
        pageSize: RESULT_PAGE_SIZE,
        mode: 'tag_result',
        tagId: args.tagId,
        tagName: args.tagName,
        level1Id: args.level1Id,
      })
    : null;

  const nextToken = hasNext
    ? await createCallbackToken({
        channelId: args.channelId,
        requesterId: args.requesterId,
        page: page + 1,
        pageSize: RESULT_PAGE_SIZE,
        mode: 'tag_result',
        tagId: args.tagId,
        tagName: args.tagName,
        level1Id: args.level1Id,
      })
    : null;

  const menuToken = await createCallbackToken({
    channelId: args.channelId,
    requesterId: args.requesterId,
    page: 1,
    pageSize: TAGS_PAGE_SIZE,
    mode: 'tag_menu',
  });

  const pageRows = result.results.slice(0, RESULT_PAGE_SIZE);
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

  return {
    ok: true as const,
    action: 'send_message' as const,
    send: {
      text: renderTagResultMessage({
        tagName: args.tagName || '未命名分类',
        page,
        pageSize: RESULT_PAGE_SIZE,
        total: result.total,
        items: renderItems,
      }),
      parseMode: 'HTML' as const,
      replyMarkup: renderTagResultKeyboard({ menuToken, prevToken, nextToken }),
    },
  };
}

function escapeHtml(input: string): string {
  return input
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

import { TaskStatus, CatalogTaskStatus } from '@prisma/client';
import { CATALOG_CHANNEL_INTERVAL_GUARD_ENABLED } from '../config/env';
import { prisma } from '../infra/prisma';
import { logError, logger } from '../logger';
import {
  editMessageTextByTelegram,
  pinMessageByTelegram,
  sendTelegramRequest,
  sendTextByTelegram,
} from '../shared/telegram';

function getFileStem(fileName: string) {
  const trimmed = fileName.trim();
  const stem = trimmed.replace(/\.[^./\\]+$/, '').trim();
  return stem || trimmed;
}

function normalizeCollectionKey(name: string) {
  return name
    .normalize('NFKC')
    .replace(/\s+/g, ' ')
    .trim();
}

function escapeHtml(text: string) {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function isAiFailureText(text?: string | null) {
  if (!text) return false;
  const normalized = text.trim();
  if (!normalized) return false;

  return /无法识别|抱歉|如果可以提供更多|视频的内容简介|主要角色|生成相关文案/.test(normalized);
}

function extractFieldValue(caption: string, fieldPattern: RegExp) {
  const match = caption.match(fieldPattern);
  if (!match) return '';
  return (match[1] || '').trim();
}

function normalizeDisplayTitle(rawTitle: string, fallbackTitle: string) {
  const title = rawTitle.trim();
  if (!title || title === '未知' || title === '待考') return fallbackTitle;

  const wrappedMatch = title.match(/《[^》]+》/);
  if (wrappedMatch) return wrappedMatch[0];

  return `《${title.replace(/^《|》$/g, '').trim()}》`;
}

function buildCatalogShortTitle(caption: string, fallbackTitle: string) {
  const parsedTitle = extractFieldValue(caption, /(?:^|\n)\s*📺?\s*片名\s*[：:]\s*(.+)/);
  const parsedActor = extractFieldValue(caption, /(?:^|\n)\s*(?:👥\s*)?主演\s*[：:]\s*(.+)/);

  if (parsedTitle) {
    const displayTitle = normalizeDisplayTitle(parsedTitle, fallbackTitle);
    if (parsedActor) {
      return `📺片名：${displayTitle} 👥主演: ${parsedActor}`;
    }
    return `📺片名：${displayTitle}`;
  }

  const parts = caption
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean);

  if (parts.length >= 2) return `${parts[0]} ${parts[1]}`;
  if (parts.length === 1) return parts[0];

  return fallbackTitle;
}

function chunkVideos<T>(items: T[], pageSize: number) {
  if (!items.length) return [] as T[][];

  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += pageSize) {
    chunks.push(items.slice(i, i + pageSize));
  }
  return chunks;
}

type CatalogVideo = {
  message_url: string;
  short_title: string;
};

function renderCatalogPageContent(args: {
  navTemplateText: string;
  channelName: string;
  videos: CatalogVideo[];
  pageNo: number;
  totalPages: number;
}) {
  let content = args.navTemplateText;
  content = content.replace(/{{channel_name}}/g, args.channelName);

  const eachRegex = /{{#each\s+videos}}([\s\S]*?){{\/each}}/g;
  content = content.replace(eachRegex, (_match, body) => {
    return args.videos
      .map((v) => {
        let text = body.replace(/{{this\.message_url}}/g, v.message_url);
        text = text.replace(/{{this\.short_title}}/g, v.short_title);
        return text;
      })
      .join('');
  });

  const beijingTimeStr = new Date(Date.now() + 8 * 3600 * 1000)
    .toISOString()
    .replace('T', ' ')
    .slice(0, 19);
  content = content.replace(/{{update_time}}/g, beijingTimeStr);

  if (args.totalPages > 1) {
    content = `${content}\n\n—— 第${args.pageNo}/${args.totalPages}页 ——`;
  }

  return content;
}

async function publishCatalogMessage(args: {
  botToken: string;
  chatId: string;
  text: string;
  existingMessageId?: number | null;
  replyMarkup?: unknown;
  clearReplyMarkup?: boolean;
}) {
  const existing = args.existingMessageId ?? null;

  if (!existing) {
    const sendResult = await sendTextByTelegram({
      botToken: args.botToken,
      chatId: args.chatId,
      text: args.text,
      replyMarkup: args.replyMarkup,
    });
    return { messageId: sendResult.messageId, isNewMessage: true };
  }

  try {
    await editMessageTextByTelegram({
      botToken: args.botToken,
      chatId: args.chatId,
      messageId: existing,
      text: args.text,
      replyMarkup: args.clearReplyMarkup ? { inline_keyboard: [] } : args.replyMarkup,
    });
    return { messageId: existing, isNewMessage: false };
  } catch (err) {
    const errObj = err as { message?: string };
    const message = (errObj.message || '').toLowerCase();

    if (message.includes('message is not modified')) {
      return { messageId: existing, isNewMessage: false };
    }

    const shouldFallbackToSend =
      message.includes('message to edit not found') ||
      message.includes('message_id_invalid') ||
      message.includes("message can't be edited") ||
      message.includes('message can\'t be edited');

    if (shouldFallbackToSend) {
      const sendResult = await sendTextByTelegram({
        botToken: args.botToken,
        chatId: args.chatId,
        text: args.text,
        replyMarkup: args.replyMarkup,
      });
      return { messageId: sendResult.messageId, isNewMessage: true };
    }

    throw err;
  }
}

function parseStoredPageMessageIds(raw: unknown) {
  if (!Array.isArray(raw)) return [] as number[];
  return raw.map((item) => Number(item)).filter((item) => Number.isInteger(item) && item > 0);
}

type CollectionNavState = {
  indexMessageId: number | null;
  indexPageMessageIds: number[];
  detailMessageIds: Record<string, number>;
  detailPageMessageIds: Record<string, number[]>;
};

function parseCollectionNavState(rawNavReplyMarkup: unknown): CollectionNavState {
  if (!rawNavReplyMarkup || typeof rawNavReplyMarkup !== 'object' || Array.isArray(rawNavReplyMarkup)) {
    return { indexMessageId: null, indexPageMessageIds: [], detailMessageIds: {}, detailPageMessageIds: {} };
  }

  const container = rawNavReplyMarkup as Record<string, unknown>;
  const state =
    container.__collectionNavState && typeof container.__collectionNavState === 'object'
      ? (container.__collectionNavState as Record<string, unknown>)
      : null;

  if (!state) return { indexMessageId: null, indexPageMessageIds: [], detailMessageIds: {}, detailPageMessageIds: {} };

  const indexMessageIdRaw = state.indexMessageId;
  const indexMessageId =
    typeof indexMessageIdRaw === 'number' && Number.isInteger(indexMessageIdRaw) && indexMessageIdRaw > 0
      ? indexMessageIdRaw
      : null;

  const indexPageMessageIds = parseStoredPageMessageIds(state.indexPageMessageIds);

  const detailRaw = state.detailMessageIds;
  const detailMessageIds: Record<string, number> = {};
  if (detailRaw && typeof detailRaw === 'object' && !Array.isArray(detailRaw)) {
    for (const [key, val] of Object.entries(detailRaw as Record<string, unknown>)) {
      const n = Number(val);
      if (Number.isInteger(n) && n > 0) {
        detailMessageIds[key] = n;
      }
    }
  }

  const detailPageRaw = state.detailPageMessageIds;
  const detailPageMessageIds: Record<string, number[]> = {};
  if (detailPageRaw && typeof detailPageRaw === 'object' && !Array.isArray(detailPageRaw)) {
    for (const [key, val] of Object.entries(detailPageRaw as Record<string, unknown>)) {
      const ids = parseStoredPageMessageIds(val);
      if (ids.length > 0) {
        detailPageMessageIds[key] = ids;
      }
    }
  }

  for (const [key, firstId] of Object.entries(detailMessageIds)) {
    if (!detailPageMessageIds[key] || detailPageMessageIds[key].length === 0) {
      detailPageMessageIds[key] = [firstId];
    }
  }

  return { indexMessageId, indexPageMessageIds, detailMessageIds, detailPageMessageIds };
}

function sanitizeJsonForPrisma(value: unknown): unknown {
  if (value === null) return null;
  if (value === undefined) return undefined;

  if (Array.isArray(value)) {
    return value
      .map((item) => sanitizeJsonForPrisma(item))
      .filter((item) => item !== undefined);
  }

  if (typeof value === 'object') {
    const result: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
      const sanitized = sanitizeJsonForPrisma(item);
      if (sanitized !== undefined) {
        result[key] = sanitized;
      }
    }
    return result;
  }

  return value;
}

function mergeCollectionNavStateIntoReplyMarkup(rawNavReplyMarkup: unknown, state: CollectionNavState | null) {
  const base =
    rawNavReplyMarkup && typeof rawNavReplyMarkup === 'object' && !Array.isArray(rawNavReplyMarkup)
      ? { ...(rawNavReplyMarkup as Record<string, unknown>) }
      : {};

  if (!state) {
    delete (base as Record<string, unknown>).__collectionNavState;
    return sanitizeJsonForPrisma(base) as Record<string, unknown>;
  }

  (base as Record<string, unknown>).__collectionNavState = {
    indexMessageId: state.indexMessageId ?? null,
    indexPageMessageIds: state.indexPageMessageIds,
    detailMessageIds: state.detailMessageIds,
    detailPageMessageIds: state.detailPageMessageIds,
    updatedAt: new Date().toISOString(),
  };

  return sanitizeJsonForPrisma(base) as Record<string, unknown>;
}

function toTelegramMessageLink(chatIdRaw: string, messageId: number): string | null {
  if (chatIdRaw.startsWith('-100')) {
    const internalId = chatIdRaw.slice(4);
    return `https://t.me/c/${internalId}/${messageId}`;
  }

  if (chatIdRaw.startsWith('@')) {
    return `https://t.me/${chatIdRaw.slice(1)}/${messageId}`;
  }

  return null;
}

function buildCatalogIndexContent(channelName: string, totalPages: number) {
  return [
    `📚 ${channelName} 目录导航`,
    `共 ${totalPages} 页，点击下方按钮跳转。`,
    `更新时间：${new Date(Date.now() + 8 * 3600 * 1000).toISOString().replace('T', ' ').slice(0, 19)}`,
  ].join('\n');
}

function buildCatalogPageButtons(args: { chatId: string; pageMessageIds: number[] }) {
  const buttons = args.pageMessageIds
    .map((messageId, index) => {
      const url = toTelegramMessageLink(args.chatId, messageId);
      if (!url) return null;
      return { text: `第${index + 1}页`, url };
    })
    .filter((button): button is { text: string; url: string } => Boolean(button));

  if (buttons.length === 0) return null;

  const inlineKeyboard: Array<Array<{ text: string; url: string }>> = [];
  for (let i = 0; i < buttons.length; i += 5) {
    inlineKeyboard.push(buttons.slice(i, i + 5));
  }

  if (buttons.length > 1) {
    inlineKeyboard.push([
      { text: '⬅️ 上一页', url: buttons[0].url },
      { text: '下一页 ➡️', url: buttons[1].url },
    ]);
  }

  return { inline_keyboard: inlineKeyboard };
}

function buildCatalogContentPageReplyMarkup(args: {
  chatId: string;
  currentPage: number;
  totalPages: number;
  pageMessageIds: number[];
}) {
  if (args.totalPages <= 1) return null;

  const inlineKeyboard: Array<Array<{ text: string; url: string }>> = [];

  const pageStart = Math.max(1, Math.min(args.currentPage - 2, args.totalPages - 4));
  const pageEnd = Math.min(args.totalPages, pageStart + 4);
  const pageNoRow: Array<{ text: string; url: string }> = [];

  for (let page = pageStart; page <= pageEnd; page += 1) {
    const messageId = args.pageMessageIds[page - 1];
    const url = messageId ? toTelegramMessageLink(args.chatId, messageId) : null;
    if (!url) continue;

    pageNoRow.push({
      text: page === args.currentPage ? `·${page}·` : String(page),
      url,
    });
  }

  if (pageNoRow.length > 0) {
    inlineKeyboard.push(pageNoRow);
  }

  const navRow: Array<{ text: string; url: string }> = [];
  if (args.currentPage > 1) {
    const prevId = args.pageMessageIds[args.currentPage - 2];
    const prevUrl = prevId ? toTelegramMessageLink(args.chatId, prevId) : null;
    if (prevUrl) navRow.push({ text: '⬅️ 上一页', url: prevUrl });
  }

  if (args.currentPage < args.totalPages) {
    const nextId = args.pageMessageIds[args.currentPage];
    const nextUrl = nextId ? toTelegramMessageLink(args.chatId, nextId) : null;
    if (nextUrl) navRow.push({ text: '下一页 ➡️', url: nextUrl });
  }

  if (navRow.length > 0) {
    inlineKeyboard.push(navRow);
  }

  return inlineKeyboard.length > 0 ? { inline_keyboard: inlineKeyboard } : null;
}

function buildCollectionIndexReplyMarkup(args: {
  chatId: string;
  pageItems: Array<{ text: string; url: string }>;
  currentPage: number;
  totalPages: number;
  indexPageMessageIds: number[];
}) {
  const inlineKeyboard: Array<Array<{ text: string; url: string }>> = args.pageItems.map((item) => [{
    text: item.text,
    url: item.url,
  }]);

  if (args.totalPages <= 1) {
    return { inline_keyboard: inlineKeyboard };
  }

  const pageStart = Math.max(1, Math.min(args.currentPage - 2, args.totalPages - 4));
  const pageEnd = Math.min(args.totalPages, pageStart + 4);
  const pageNoRow: Array<{ text: string; url: string }> = [];

  for (let page = pageStart; page <= pageEnd; page += 1) {
    const messageId = args.indexPageMessageIds[page - 1];
    const url = messageId ? toTelegramMessageLink(args.chatId, messageId) : null;
    if (!url) continue;

    pageNoRow.push({
      text: page === args.currentPage ? `·${page}·` : String(page),
      url,
    });
  }

  if (pageNoRow.length > 0) {
    inlineKeyboard.push(pageNoRow);
  }

  const navRow: Array<{ text: string; url: string }> = [];

  const prevMessageId = args.currentPage > 1 ? args.indexPageMessageIds[args.currentPage - 2] : null;
  const prevUrl = prevMessageId ? toTelegramMessageLink(args.chatId, prevMessageId) : null;
  if (prevUrl) {
    navRow.push({ text: '⬅️ 上一页', url: prevUrl });
  }

  const nextMessageId = args.currentPage < args.totalPages ? args.indexPageMessageIds[args.currentPage] : null;
  const nextUrl = nextMessageId ? toTelegramMessageLink(args.chatId, nextMessageId) : null;
  if (nextUrl) {
    navRow.push({ text: '下一页 ➡️', url: nextUrl });
  }

  if (navRow.length > 0) {
    inlineKeyboard.push(navRow);
  }

  return { inline_keyboard: inlineKeyboard };
}

function buildCollectionDetailReplyMarkup(args: {
  chatId: string;
  currentPage: number;
  totalPages: number;
  detailPageMessageIds: number[];
}) {
  if (args.totalPages <= 1) return null;

  const inlineKeyboard: Array<Array<{ text: string; url: string }>> = [];

  const pageStart = Math.max(1, Math.min(args.currentPage - 2, args.totalPages - 4));
  const pageEnd = Math.min(args.totalPages, pageStart + 4);
  const pageNoRow: Array<{ text: string; url: string }> = [];

  for (let page = pageStart; page <= pageEnd; page += 1) {
    const messageId = args.detailPageMessageIds[page - 1];
    const url = messageId ? toTelegramMessageLink(args.chatId, messageId) : null;
    if (!url) continue;

    pageNoRow.push({
      text: page === args.currentPage ? `·${page}·` : String(page),
      url,
    });
  }

  if (pageNoRow.length > 0) inlineKeyboard.push(pageNoRow);

  const navRow: Array<{ text: string; url: string }> = [];
  if (args.currentPage > 1) {
    const prevId = args.detailPageMessageIds[args.currentPage - 2];
    const prevUrl = prevId ? toTelegramMessageLink(args.chatId, prevId) : null;
    if (prevUrl) navRow.push({ text: '⬅️ 上一页', url: prevUrl });
  }

  if (args.currentPage < args.totalPages) {
    const nextId = args.detailPageMessageIds[args.currentPage];
    const nextUrl = nextId ? toTelegramMessageLink(args.chatId, nextId) : null;
    if (nextUrl) navRow.push({ text: '下一页 ➡️', url: nextUrl });
  }

  if (navRow.length > 0) inlineKeyboard.push(navRow);

  return inlineKeyboard.length > 0 ? { inline_keyboard: inlineKeyboard } : null;
}

function parseCollectionMeta(sourceMeta: unknown) {
  if (!sourceMeta || typeof sourceMeta !== 'object') return null;
  const meta = sourceMeta as Record<string, unknown>;
  if (meta.isCollection !== true) return null;

  const collectionName = typeof meta.collectionName === 'string' ? meta.collectionName : '';
  const episodeNo =
    typeof meta.episodeNo === 'number'
      ? meta.episodeNo
      : typeof meta.episodeNo === 'string' && /^\d+$/.test(meta.episodeNo)
        ? Number(meta.episodeNo)
        : null;

  if (!collectionName || episodeNo === null) return null;

  return {
    collectionName,
    episodeNo,
  };
}

export async function handleCatalogJob(channelIdRaw: string) {
  let catalogTaskId: bigint | null = null;
  const channelId = BigInt(channelIdRaw);

  const channel = await prisma.channel.findUnique({
    where: { id: channelId },
    include: {
      defaultBot: {
        select: { id: true, status: true, tokenEncrypted: true },
      },
    },
  });

  if (!channel) throw new Error(`未找到频道: ${channelIdRaw}`);
  if (!channel.navEnabled || channel.status !== 'active') {
    return { ok: true, skipped: true, reason: '导航未启用或频道未激活' };
  }
  if (!channel.navTemplateText || !channel.defaultBot) {
    throw new Error(`频道缺少导航模板或默认机器人: ${channelIdRaw}`);
  }
  if (channel.defaultBot.status !== 'active') {
    throw new Error(`频道机器人未启用: ${channelIdRaw}`);
  }

  if (CATALOG_CHANNEL_INTERVAL_GUARD_ENABLED) {
    const guardNow = new Date();
    const intervalSec = Math.max(0, channel.navIntervalSec ?? 0);
    const nextAllowedAt = channel.lastNavUpdateAt
      ? new Date(channel.lastNavUpdateAt.getTime() + intervalSec * 1000)
      : guardNow;

    if (nextAllowedAt.getTime() > guardNow.getTime()) {
      return {
        ok: true,
        skipped: true,
        reason: 'nav_interval_not_due',
        channelId: channelIdRaw,
        nextAllowedAt: nextAllowedAt.toISOString(),
      };
    }
  }

  const bot = channel.defaultBot;

  let catalogTemplate = await prisma.catalogTemplate.findFirst({
    where: { isActive: true },
    select: { id: true },
    orderBy: { createdAt: 'desc' },
  });

  if (!catalogTemplate) {
    catalogTemplate = await prisma.catalogTemplate.create({
      data: {
        name: '默认模板',
        bodyTemplate: '{{#each videos}}\\n{{this.short_title}}\\n{{this.message_url}}\\n{{/each}}',
        recentLimit: 60,
        isActive: true,
      },
      select: { id: true },
    });
  }

  const now = new Date();
  const reuseWindowMs = 2 * 60 * 1000;
  const recentExisting = await prisma.catalogTask.findFirst({
    where: {
      channelId,
      status: { in: [CatalogTaskStatus.pending, CatalogTaskStatus.running] },
    },
    orderBy: { createdAt: 'desc' },
    select: { id: true, createdAt: true },
  });

  if (recentExisting && now.getTime() - recentExisting.createdAt.getTime() <= reuseWindowMs) {
    catalogTaskId = recentExisting.id;
  } else {
    const created = await prisma.catalogTask.create({
      data: {
        channelId,
        catalogTemplateId: catalogTemplate.id,
        status: CatalogTaskStatus.pending,
        plannedAt: now,
        pinAfterPublish: false,
      },
      select: { id: true },
    });
    catalogTaskId = created.id;
  }

  await prisma.catalogTask.update({
    where: { id: catalogTaskId },
    data: { status: CatalogTaskStatus.running, startedAt: now },
  });

  const recentLimit = channel.navRecentLimit ?? 60;
  const dispatchTasks = await prisma.dispatchTask.findMany({
    where: { channelId, status: TaskStatus.success, telegramMessageId: { not: null } },
    orderBy: { finishedAt: 'desc' },
    take: recentLimit,
    select: {
      caption: true,
      telegramMessageLink: true,
      mediaAsset: {
        select: {
          originalName: true,
          sourceMeta: true,
        },
      },
    },
  });

  if (dispatchTasks.length === 0) {
    if (catalogTaskId) {
      await prisma.catalogTask.update({
        where: { id: catalogTaskId },
        data: {
          status: CatalogTaskStatus.cancelled,
          finishedAt: new Date(),
          errorMessage: '没有可用的成功分发记录',
        },
      });
    }
    return { ok: true, skipped: true, reason: '没有可用的成功分发记录' };
  }

  const orderedDispatchTasks = [...dispatchTasks].reverse();
  const channelAny = channel as any;
  const collectionNavEnabled =
    typeof channelAny.collection_nav_enabled === 'boolean' ? channelAny.collection_nav_enabled : true;

  const regularDispatchTasks = collectionNavEnabled
    ? orderedDispatchTasks.filter((task) => !parseCollectionMeta(task.mediaAsset?.sourceMeta))
    : orderedDispatchTasks;

  const videos = regularDispatchTasks.map((t) => {
    const sourceMeta =
      t.mediaAsset?.sourceMeta && typeof t.mediaAsset.sourceMeta === 'object'
        ? (t.mediaAsset.sourceMeta as Record<string, unknown>)
        : {};
    const customCatalogTitle =
      typeof sourceMeta.catalogCustomTitle === 'string' ? sourceMeta.catalogCustomTitle.trim() : '';
    const fallbackTitle = getFileStem(t.mediaAsset?.originalName || '未命名视频');
    const safeCaption = isAiFailureText(t.caption) ? fallbackTitle : (t.caption || '').trim();

    let shortTitle = buildCatalogShortTitle(safeCaption, fallbackTitle);

    if (isAiFailureText(shortTitle)) {
      shortTitle = fallbackTitle;
    }

    return {
      message_url: t.telegramMessageLink || '',
      short_title: customCatalogTitle || shortTitle,
    };
  });

  const collectionGroups = new Map<
    string,
    Array<{ episodeNo: number; title: string; messageUrl: string; isMissingPlaceholder?: boolean }>
  >();
  for (const task of orderedDispatchTasks) {
    if (!task.telegramMessageLink) continue;
    const meta = parseCollectionMeta(task.mediaAsset?.sourceMeta);
    if (!meta) continue;

    const fileNameTitle = getFileStem(task.mediaAsset?.originalName || '未命名视频');

    const group = collectionGroups.get(meta.collectionName) ?? [];
    group.push({
      episodeNo: meta.episodeNo,
      title: fileNameTitle,
      messageUrl: task.telegramMessageLink,
    });
    collectionGroups.set(meta.collectionName, group);
  }

  const skippedCollectionAssets = await prisma.mediaAsset.findMany({
    where: {
      channelId,
      AND: [
        { sourceMeta: { path: ['isCollection'], equals: true } },
        { sourceMeta: { path: ['skipStatus'], equals: 'skipped_missing' } },
      ],
    },
    select: {
      sourceMeta: true,
    },
  });

  for (const asset of skippedCollectionAssets) {
    const meta = parseCollectionMeta(asset.sourceMeta);
    if (!meta || meta.episodeNo === null) continue;

    const group = collectionGroups.get(meta.collectionName) ?? [];
    const hasRealEpisode = group.some((item) => item.episodeNo === meta.episodeNo && !item.isMissingPlaceholder);
    if (!hasRealEpisode) {
      group.push({
        episodeNo: meta.episodeNo,
        title: `第${meta.episodeNo}集（暂缺）`,
        messageUrl: '',
        isMissingPlaceholder: true,
      });
      collectionGroups.set(meta.collectionName, group);
    }
  }

  const navPageSize = Math.max(1, Math.min(100, (channel as any).navPageSize ?? 10));
  const navPagingEnabled = typeof (channel as any).navPagingEnabled === 'boolean'
    ? (channel as any).navPagingEnabled
    : false;
  logger.info('[q_catalog] 主目录分页参数', {
    channelId: channelIdRaw,
    navPagingEnabled,
    channelNavPageSize: navPageSize,
    regularVideoCount: videos.length,
  });
  const videoPages = navPagingEnabled ? chunkVideos(videos, navPageSize) : [videos];
  const pageContents = videoPages.map((pageVideos, index) =>
    renderCatalogPageContent({
      navTemplateText: channel.navTemplateText!,
      channelName: channel.name,
      videos: pageVideos,
      pageNo: index + 1,
      totalPages: videoPages.length,
    }),
  );

  const botToken = bot.tokenEncrypted;
  let finalMessageId: number | null = null;

  const collectionSections: string[] = [];
  let collectionIndexLinkInMainCatalog: string | null = null;
  let nextCollectionNavState: CollectionNavState | null = null;
  if (collectionGroups.size > 0) {
    const names = [...collectionGroups.keys()].sort((a, b) => a.localeCompare(b, 'zh-Hans-CN'));
    const existingNavState = parseCollectionNavState(channel.navReplyMarkup);
    const detailMessageIds: Record<string, number> = {};
    const detailPageMessageIds: Record<string, number[]> = {};

    const collectionIndexPageSize = Math.max(1, Math.min(50, Number(channelAny.collection_index_page_size ?? 20)));

    const collectionNameToNormalized = new Map(names.map((name) => [name, normalizeCollectionKey(name)]));
    const normalizedNames = [...new Set(collectionNameToNormalized.values())];

    let collectionConfigs: Array<{ name: string; navPageSize: number | null }> = [];
    const collectionModel = (prisma as any).collection;
    if (!collectionModel) {
      logger.warn('[q_catalog] Prisma collection 模型缺失，跳过合集配置读取', {
        channelId: channelIdRaw,
      });
    } else {
      const allChannelCollections: Array<{ name: string; navPageSize: number | null }> = await collectionModel.findMany({
        where: { channelId },
        select: {
          name: true,
          navPageSize: true,
        },
      });

      const requestedNameSet = new Set(names);
      const requestedNormalizedNameSet = new Set(normalizedNames);
      collectionConfigs = allChannelCollections.filter((item) => {
        const normalizedItemName = normalizeCollectionKey(item.name);
        return requestedNameSet.has(item.name) || requestedNormalizedNameSet.has(normalizedItemName);
      });

      logger.info('[q_catalog] 合集配置原始读取结果(按频道)', {
        channelId: channelIdRaw,
        fetchedCount: allChannelCollections.length,
        fetchedNames: allChannelCollections.map((item) => item.name),
      });
    }

    const collectionNavPageSizeMap = new Map<string, number>();
    for (const item of collectionConfigs) {
      const pageSize = Math.max(1, Math.min(100, Number(item.navPageSize ?? navPageSize)));
      collectionNavPageSizeMap.set(item.name, pageSize);
      collectionNavPageSizeMap.set(normalizeCollectionKey(item.name), pageSize);
    }

    logger.info('[q_catalog] 合集分页配置命中情况', {
      channelId: channelIdRaw,
      collectionCount: names.length,
      matchedConfigCount: collectionConfigs.length,
      channelFallbackPageSize: navPageSize,
      requestedNames: names,
      normalizedNames,
      collectionConfigs,
      querySummary: {
        model: 'collection',
        where: {
          channelId,
          mode: 'findMany(channelId)->memory_match(name|normalizedName)',
          namesCount: names.length,
          normalizedNamesCount: normalizedNames.length,
        },
      },
    });

    const collectionIndexItems: Array<{ text: string; url: string }> = [];

    for (let idx = 0; idx < names.length; idx += 1) {
      const name = names[idx];
      const episodes = (collectionGroups.get(name) ?? []).sort((a, b) => a.episodeNo - b.episodeNo);
      const normalizedName = collectionNameToNormalized.get(name) ?? normalizeCollectionKey(name);
      const hasCollectionSpecificPageSize =
        collectionNavPageSizeMap.has(name) || collectionNavPageSizeMap.has(normalizedName);
      const collectionDetailPageSize =
        collectionNavPageSizeMap.get(name) ??
        collectionNavPageSizeMap.get(normalizedName) ??
        Math.max(1, Math.min(100, navPageSize));
      logger.info('[q_catalog] 合集详情分页参数', {
        channelId: channelIdRaw,
        collectionName: name,
        episodeCount: episodes.length,
        collectionDetailPageSize,
        source: hasCollectionSpecificPageSize ? 'collection.navPageSize' : 'channel.navPageSize_fallback',
      });
      const episodePages = chunkVideos(episodes, collectionDetailPageSize);
      const existingDetailPages = existingNavState.detailPageMessageIds[name] ?? [];
      const publishedDetailPages: number[] = [];

      const detailTexts: string[] = [];

      for (let pageIndex = 0; pageIndex < episodePages.length; pageIndex += 1) {
        const pageLines = episodePages[pageIndex].map((ep) => {
          const displayTitle = getFileStem(ep.title).trim();
          const safeTitle = escapeHtml(displayTitle || `第${ep.episodeNo}集`);

          if (ep.isMissingPlaceholder || !ep.messageUrl) {
            return safeTitle;
          }

          const safeUrl = escapeHtml(ep.messageUrl);
          return `<a href="${safeUrl}">${safeTitle}</a>`;
        });

        const detailText = [
          `📺 ${escapeHtml(name)}`,
          ...pageLines,
          ...(episodePages.length > 1 ? [`—— 第${pageIndex + 1}/${episodePages.length}页 ——`] : []),
        ].join('\n');

        detailTexts.push(detailText);

        const detailPublishResult = await publishCatalogMessage({
          botToken,
          chatId: channel.tgChatId,
          text: detailText,
          existingMessageId: existingDetailPages[pageIndex] ?? null,
        });

        publishedDetailPages.push(detailPublishResult.messageId);
      }

      if (episodePages.length > 1) {
        for (let pageIndex = 0; pageIndex < episodePages.length; pageIndex += 1) {
          const currentMessageId = publishedDetailPages[pageIndex] ?? null;
          if (!currentMessageId) continue;

          await publishCatalogMessage({
            botToken,
            chatId: channel.tgChatId,
            text: detailTexts[pageIndex],
            existingMessageId: currentMessageId,
            replyMarkup: buildCollectionDetailReplyMarkup({
              chatId: channel.tgChatId,
              currentPage: pageIndex + 1,
              totalPages: episodePages.length,
              detailPageMessageIds: publishedDetailPages,
            }) ?? undefined,
          });
        }
      }

      const staleDetailPages = existingDetailPages.slice(publishedDetailPages.length);
      for (const staleMessageId of staleDetailPages) {
        try {
          await sendTelegramRequest({
            botToken,
            method: 'deleteMessage',
            payload: {
              chat_id: channel.tgChatId,
              message_id: staleMessageId,
            },
          });
        } catch (deleteError) {
          logError('[q_catalog] 删除旧合集详情分页消息失败', {
            channelId: channelIdRaw,
            collectionName: name,
            staleMessageId,
            error: deleteError,
          });
        }
      }

      if (publishedDetailPages.length > 0) {
        detailMessageIds[name] = publishedDetailPages[0];
        detailPageMessageIds[name] = publishedDetailPages;
      }

      const detailLink = publishedDetailPages[0]
        ? toTelegramMessageLink(channel.tgChatId, publishedDetailPages[0])
        : null;
      if (detailLink) {
        collectionIndexItems.push({ text: `${idx + 1}: ${name}`, url: detailLink });
      }
    }

    const indexPages = chunkVideos(collectionIndexItems, collectionIndexPageSize);
    const existingIndexPages = existingNavState.indexPageMessageIds;
    const publishedIndexPages: number[] = [];

    for (let pageIndex = 0; pageIndex < indexPages.length; pageIndex += 1) {
      const text = [
        '📚 合集索引',
        '请选择合集：',
        ...(indexPages.length > 1 ? [`—— 第${pageIndex + 1}/${indexPages.length}页 ——`] : []),
      ].join('\n');

      const indexPublishResult = await publishCatalogMessage({
        botToken,
        chatId: channel.tgChatId,
        text,
        existingMessageId: existingIndexPages[pageIndex] ?? null,
        replyMarkup: null,
      });

      publishedIndexPages.push(indexPublishResult.messageId);
    }

    for (let pageIndex = 0; pageIndex < indexPages.length; pageIndex += 1) {
      const currentMessageId = publishedIndexPages[pageIndex] ?? null;
      if (!currentMessageId) continue;

      const text = [
        '📚 合集索引',
        '请选择合集：',
        ...(indexPages.length > 1 ? [`—— 第${pageIndex + 1}/${indexPages.length}页 ——`] : []),
      ].join('\n');

      const replyMarkup = buildCollectionIndexReplyMarkup({
        chatId: channel.tgChatId,
        pageItems: indexPages[pageIndex],
        currentPage: pageIndex + 1,
        totalPages: indexPages.length,
        indexPageMessageIds: publishedIndexPages,
      });

      await publishCatalogMessage({
        botToken,
        chatId: channel.tgChatId,
        text,
        existingMessageId: currentMessageId,
        replyMarkup,
      });
    }

    const staleIndexPages = existingIndexPages.slice(publishedIndexPages.length);
    for (const staleMessageId of staleIndexPages) {
      try {
        await sendTelegramRequest({
          botToken,
          method: 'deleteMessage',
          payload: {
            chat_id: channel.tgChatId,
            message_id: staleMessageId,
          },
        });
      } catch (deleteError) {
        logError('[q_catalog] 删除旧合集索引分页消息失败', {
          channelId: channelIdRaw,
          staleMessageId,
          error: deleteError,
        });
      }
    }

    const indexMessageId = publishedIndexPages[0] ?? null;
    collectionIndexLinkInMainCatalog = indexMessageId
      ? toTelegramMessageLink(channel.tgChatId, indexMessageId)
      : null;

    const staleCollectionNames = Object.keys(existingNavState.detailPageMessageIds).filter(
      (name) => !Object.prototype.hasOwnProperty.call(detailPageMessageIds, name),
    );
    for (const staleName of staleCollectionNames) {
      const stalePages = existingNavState.detailPageMessageIds[staleName] ?? [];
      for (const staleMessageId of stalePages) {
        try {
          await sendTelegramRequest({
            botToken,
            method: 'deleteMessage',
            payload: {
              chat_id: channel.tgChatId,
              message_id: staleMessageId,
            },
          });
        } catch (deleteError) {
          logError('[q_catalog] 删除旧合集详情消息失败', {
            channelId: channelIdRaw,
            collectionName: staleName,
            staleMessageId,
            error: deleteError,
          });
        }
      }
    }

    nextCollectionNavState = {
      indexMessageId,
      indexPageMessageIds: publishedIndexPages,
      detailMessageIds,
      detailPageMessageIds,
    };
  }

  const mainCatalogContent = (() => {
    const base = pageContents.join('\n\n');
    if (!collectionNavEnabled || !collectionIndexLinkInMainCatalog) return base;
    const safeLink = escapeHtml(collectionIndexLinkInMainCatalog);
    return `${base}\n\n<a href="${safeLink}">📚 合集索引</a>`;
  })();

  const content = [mainCatalogContent, ...collectionSections].filter(Boolean).join('\n\n');
  const contentPreview = content.slice(0, 4000);
  let pinAttempted = false;
  let pinSuccess: boolean | null = null;
  let pinErrorMessage: string | null = null;

  try {
    const channelAny = channel as any;
    const storedPageMessageIds = parseStoredPageMessageIds(channelAny.navPageMessageIds);
    const publishedPageMessageIds: number[] = [];

    if (pageContents.length <= 1) {
      const publishResult = await publishCatalogMessage({
        botToken,
        chatId: channel.tgChatId,
        text: mainCatalogContent,
        existingMessageId: channel.navMessageId ? Number(channel.navMessageId) : null,
        clearReplyMarkup: true,
      });

      finalMessageId = publishResult.messageId;

      if (publishResult.isNewMessage) {
        pinAttempted = true;
        try {
          await pinMessageByTelegram({
            botToken,
            chatId: channel.tgChatId,
            messageId: finalMessageId,
          });
          pinSuccess = true;
        } catch (pinErr) {
          pinSuccess = false;
          pinErrorMessage = pinErr instanceof Error ? pinErr.message : '置顶失败';
        }
      }
    } else {
      for (let pageIndex = 0; pageIndex < pageContents.length; pageIndex += 1) {
        const pageText = pageIndex === 0 ? mainCatalogContent : pageContents[pageIndex];
        const publishResult = await publishCatalogMessage({
          botToken,
          chatId: channel.tgChatId,
          text: pageText,
          existingMessageId: storedPageMessageIds[pageIndex] ?? null,
        });
        publishedPageMessageIds.push(publishResult.messageId);
      }

      for (let pageIndex = 0; pageIndex < pageContents.length; pageIndex += 1) {
        const pageText = pageIndex === 0 ? mainCatalogContent : pageContents[pageIndex];
        await publishCatalogMessage({
          botToken,
          chatId: channel.tgChatId,
          text: pageText,
          existingMessageId: publishedPageMessageIds[pageIndex],
          replyMarkup: buildCatalogContentPageReplyMarkup({
            chatId: channel.tgChatId,
            currentPage: pageIndex + 1,
            totalPages: pageContents.length,
            pageMessageIds: publishedPageMessageIds,
          }) ?? undefined,
        });
      }

      const indexReplyMarkup = buildCatalogPageButtons({
        chatId: channel.tgChatId,
        pageMessageIds: publishedPageMessageIds,
      });

      const indexPublishResult = await publishCatalogMessage({
        botToken,
        chatId: channel.tgChatId,
        text: buildCatalogIndexContent(channel.name, pageContents.length),
        existingMessageId: channel.navMessageId ? Number(channel.navMessageId) : null,
        replyMarkup: indexReplyMarkup,
      });

      finalMessageId = indexPublishResult.messageId;

      if (indexPublishResult.isNewMessage) {
        pinAttempted = true;
        try {
          await pinMessageByTelegram({
            botToken,
            chatId: channel.tgChatId,
            messageId: finalMessageId,
          });
          pinSuccess = true;
        } catch (pinErr) {
          pinSuccess = false;
          pinErrorMessage = pinErr instanceof Error ? pinErr.message : '置顶失败';
        }
      }
    }

    const stalePageMessageIds = storedPageMessageIds.slice(publishedPageMessageIds.length);
    for (const staleMessageId of stalePageMessageIds) {
      try {
        await sendTelegramRequest({
          botToken,
          method: 'deleteMessage',
          payload: {
            chat_id: channel.tgChatId,
            message_id: staleMessageId,
          },
        });
      } catch (deleteError) {
        logError('[q_catalog] 删除旧目录分页消息失败', {
          channelId: channelIdRaw,
          staleMessageId,
          error: deleteError,
        });
      }
    }

    try {
      await prisma.channel.update({
        where: { id: channelId },
        data: {
          navMessageId: finalMessageId ? BigInt(finalMessageId) : null,
          lastNavUpdateAt: new Date(),
          navReplyMarkup: mergeCollectionNavStateIntoReplyMarkup(channel.navReplyMarkup, nextCollectionNavState),
          ...({ navPageMessageIds: publishedPageMessageIds } as any),
        } as any,
      });
    } catch (updateError) {
      const updateMessage = updateError instanceof Error ? updateError.message : '';
      if (updateMessage.includes('Unknown argument `navPageMessageIds`')) {
        await prisma.channel.update({
          where: { id: channelId },
          data: {
            navMessageId: finalMessageId ? BigInt(finalMessageId) : null,
            lastNavUpdateAt: new Date(),
            navReplyMarkup: mergeCollectionNavStateIntoReplyMarkup(channel.navReplyMarkup, nextCollectionNavState) as any,
          },
        });
      } else {
        throw updateError;
      }
    }

    await prisma.catalogHistory.create({
      data: {
        channelId,
        catalogTemplateId: catalogTemplate.id,
        content,
        renderedCount: videos.length,
        publishedAt: new Date(),
      },
    });

    if (catalogTaskId) {
      await prisma.catalogTask.update({
        where: { id: catalogTaskId },
        data: {
          status: CatalogTaskStatus.success,
          finishedAt: new Date(),
          telegramMessageId: finalMessageId ? BigInt(finalMessageId) : null,
          contentPreview,
          errorMessage: null,
          pinAfterPublish: pinAttempted,
          pinSuccess,
          pinErrorMessage,
        },
      });
    }

    return {
      ok: true,
      channelId: channelIdRaw,
      messageId: finalMessageId,
      pageCount: pageContents.length,
      navPageSize,
    };
  } catch (error) {
    if (catalogTaskId) {
      await prisma.catalogTask.update({
        where: { id: catalogTaskId },
        data: {
          status: CatalogTaskStatus.failed,
          finishedAt: new Date(),
          contentPreview,
          errorMessage: error instanceof Error ? error.message : '频道导航发布失败',
          pinAfterPublish: pinAttempted,
          pinSuccess,
          pinErrorMessage:
            pinErrorMessage ?? (error instanceof Error ? error.message : '频道导航发布失败'),
        },
      });
    }

    logError('[q_catalog] 发布频道导航失败', {
      channelId: channelIdRaw,
      errorName: error instanceof Error ? error.name : 'UnknownError',
      errorMessage: error instanceof Error ? error.message : String(error),
      errorStack: error instanceof Error ? error.stack : null,
      error,
    });
    throw error;
  }
}

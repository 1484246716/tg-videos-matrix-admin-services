import { TaskStatus, CatalogTaskStatus } from '@prisma/client';
import {
  CATALOG_CHANNEL_INTERVAL_GUARD_ENABLED,
  TYPEC_COLLECTION_CACHE_FALLBACK_TO_DB,
  TYPEC_COLLECTION_CACHE_STALE_SECONDS,
  TYPEC_COLLECTION_DATA_SOURCE,
  TYPEC_COLLECTION_FULL_SCAN_BATCH_SIZE,
  TYPEC_COLLECTION_INDEX_SHOW_EMPTY,
  TYPEC_HASH_FORCE_REPUBLISH_ON_VERSION_CHANGE,
  TYPEC_HASH_GATE_ENABLED,
  TYPEC_HASH_SCHEMA_VERSION,
  TYPEC_READ_FROM_CATALOG_SOURCE,
  TYPEC_SELF_HEAL_ON_RUN,
} from '../config/env';
import { prisma } from '../infra/prisma';
import { createHash, randomUUID } from 'node:crypto';
import { logError, logger } from '../logger';
import { releaseChannelLock, tryAcquireChannelLock } from '../shared/channel-lock';
import { catalogMetrics } from '../shared/metrics';
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
    return { messageId: sendResult.messageId, isNewMessage: true, fallbackSent: false, notModified: false };
  }

  try {
    await editMessageTextByTelegram({
      botToken: args.botToken,
      chatId: args.chatId,
      messageId: existing,
      text: args.text,
      replyMarkup: args.clearReplyMarkup ? { inline_keyboard: [] } : args.replyMarkup,
    });
    return { messageId: existing, isNewMessage: false, fallbackSent: false, notModified: false };
  } catch (err) {
    const errObj = err as { message?: string };
    const message = (errObj.message || '').toLowerCase();

    if (message.includes('message is not modified')) {
      return { messageId: existing, isNewMessage: false, fallbackSent: false, notModified: true };
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
      return { messageId: sendResult.messageId, isNewMessage: true, fallbackSent: true, notModified: false };
    }

    throw err;
  }
}

function parseStoredPageMessageIds(raw: unknown) {
  if (!Array.isArray(raw)) return [] as number[];
  return raw.map((item) => Number(item)).filter((item) => Number.isInteger(item) && item > 0);
}

function reconcilePageMessageIds(args: {
  ids: number[];
  expectedCount: number;
  scope: string;
  channelIdRaw: string;
}) {
  const valid = args.ids.filter((item) => Number.isInteger(item) && item > 0);
  const deduped: number[] = [];
  const seen = new Set<number>();
  for (const id of valid) {
    if (seen.has(id)) continue;
    seen.add(id);
    deduped.push(id);
  }

  if (deduped.length !== args.expectedCount) {
    throw new Error(
      `[q_catalog] 分页消息ID对账失败(scope=${args.scope}, channelId=${args.channelIdRaw}): expected=${args.expectedCount}, actual=${deduped.length}`,
    );
  }

  return deduped;
}

function applyPublishResultAndRewriteId(args: {
  pageIds: number[];
  pageIndex: number;
  resultMessageId: number;
  orphanCandidateIds: Set<number>;
  scope: string;
  channelIdRaw: string;
}) {
  const oldId = args.pageIds[args.pageIndex] ?? null;
  if (oldId && oldId !== args.resultMessageId) {
    args.orphanCandidateIds.add(oldId);
    logger.info('[q_catalog] 分页消息触发fallback并回写新ID', {
      channelId: args.channelIdRaw,
      scope: args.scope,
      pageIndex: args.pageIndex + 1,
      oldMessageId: oldId,
      newMessageId: args.resultMessageId,
    });
  }

  args.pageIds[args.pageIndex] = args.resultMessageId;
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

function normalizeTextForHash(text: string): string {
  return text.replace(/\r\n?/g, '\n').replace(/[ \t]+$/gm, '').trimEnd();
}

function normalizeMarkupForHash(markup: unknown): unknown {
  if (markup === null || markup === undefined) return null;
  if (Array.isArray(markup)) return markup.map((item) => normalizeMarkupForHash(item));
  if (typeof markup !== 'object') return markup;

  const obj = markup as Record<string, unknown>;
  const out: Record<string, unknown> = {};
  for (const key of Object.keys(obj).sort()) {
    const value = normalizeMarkupForHash(obj[key]);
    if (value !== undefined) out[key] = value;
  }
  return out;
}

function sha256(text: string): string {
  return createHash('sha256').update(text).digest('hex');
}

function buildPageCombinedHash(args: { text: string; replyMarkup?: unknown; schemaVersion: number }) {
  const normalizedText = normalizeTextForHash(args.text);
  const normalizedMarkup = normalizeMarkupForHash(args.replyMarkup ?? null);
  const payload = JSON.stringify({
    schemaVersion: args.schemaVersion,
    text: normalizedText,
    replyMarkup: normalizedMarkup,
  });
  const combinedHash = sha256(payload);
  return { combinedHash, normalizedText, normalizedMarkup };
}

type CatalogHashRecord = {
  schemaVersion: number;
  combinedHash: string;
  updatedAt: string;
};

type CatalogHashState = {
  main_catalog?: Record<string, CatalogHashRecord>;
  collection_index?: Record<string, CatalogHashRecord>;
  collection_detail?: Record<string, Record<string, CatalogHashRecord>>;
};

function parseCatalogHashState(rawNavReplyMarkup: unknown): CatalogHashState {
  if (!rawNavReplyMarkup || typeof rawNavReplyMarkup !== 'object' || Array.isArray(rawNavReplyMarkup)) {
    return {};
  }
  const container = rawNavReplyMarkup as Record<string, unknown>;
  const raw = container.__catalogHashState;
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
  return raw as CatalogHashState;
}

function mergeCatalogHashStateIntoReplyMarkup(rawNavReplyMarkup: unknown, hashState: CatalogHashState | null) {
  const base =
    rawNavReplyMarkup && typeof rawNavReplyMarkup === 'object' && !Array.isArray(rawNavReplyMarkup)
      ? { ...(rawNavReplyMarkup as Record<string, unknown>) }
      : {};

  if (!hashState) {
    delete (base as Record<string, unknown>).__catalogHashState;
    return sanitizeJsonForPrisma(base) as Record<string, unknown>;
  }

  (base as Record<string, unknown>).__catalogHashState = hashState;
  return sanitizeJsonForPrisma(base) as Record<string, unknown>;
}

function readHashRecord(
  hashState: CatalogHashState,
  scope: 'main_catalog' | 'collection_index' | 'collection_detail',
  pageNo: number,
  collectionName?: string,
): CatalogHashRecord | null {
  const key = String(pageNo);
  if (scope === 'collection_detail') {
    if (!collectionName) return null;
    return hashState.collection_detail?.[collectionName]?.[key] ?? null;
  }
  return (hashState[scope] as Record<string, CatalogHashRecord> | undefined)?.[key] ?? null;
}

function writeHashRecord(
  hashState: CatalogHashState,
  args: {
    scope: 'main_catalog' | 'collection_index' | 'collection_detail';
    pageNo: number;
    combinedHash: string;
    schemaVersion: number;
    collectionName?: string;
  },
) {
  const key = String(args.pageNo);
  const record: CatalogHashRecord = {
    schemaVersion: args.schemaVersion,
    combinedHash: args.combinedHash,
    updatedAt: new Date().toISOString(),
  };

  if (args.scope === 'collection_detail') {
    const collectionName = args.collectionName ?? '';
    hashState.collection_detail = hashState.collection_detail ?? {};
    hashState.collection_detail[collectionName] = hashState.collection_detail[collectionName] ?? {};
    hashState.collection_detail[collectionName][key] = record;
    return;
  }

  hashState[args.scope] = hashState[args.scope] ?? {};
  (hashState[args.scope] as Record<string, CatalogHashRecord>)[key] = record;
}

function shouldSkipByHash(args: {
  enabled: boolean;
  forceRepublish: boolean;
  existingMessageId: number | null;
  oldRecord: CatalogHashRecord | null;
  newCombinedHash: string;
  schemaVersion: number;
}) {
  if (!args.enabled) return false;
  if (args.forceRepublish) return false;
  if (!args.existingMessageId || args.existingMessageId <= 0) return false;
  if (!args.oldRecord) return false;
  if (args.oldRecord.combinedHash !== args.newCombinedHash) return false;
  if (
    TYPEC_HASH_FORCE_REPUBLISH_ON_VERSION_CHANGE &&
    Number(args.oldRecord.schemaVersion) !== Number(args.schemaVersion)
  ) {
    return false;
  }
  return true;
}

function pruneHashScopeRecords(records: Record<string, CatalogHashRecord> | undefined, maxPageNoInclusive: number) {
  if (!records) return;
  for (const key of Object.keys(records)) {
    const pageNo = Number(key);
    if (!Number.isInteger(pageNo) || pageNo <= 0 || pageNo > maxPageNoInclusive) {
      delete records[key];
    }
  }
}

function pruneCatalogHashState(args: {
  hashState: CatalogHashState;
  mainPageCount: number;
  indexPageCount: number;
  detailPageCountByCollection: Record<string, number>;
}) {
  pruneHashScopeRecords(args.hashState.main_catalog, args.mainPageCount);
  pruneHashScopeRecords(args.hashState.collection_index, args.indexPageCount);

  if (!args.hashState.collection_detail) return;

  for (const [collectionName, records] of Object.entries(args.hashState.collection_detail)) {
    const maxCount = args.detailPageCountByCollection[collectionName];
    if (!maxCount || maxCount <= 0) {
      delete args.hashState.collection_detail[collectionName];
      continue;
    }
    pruneHashScopeRecords(records, maxCount);
    if (Object.keys(records || {}).length === 0) {
      delete args.hashState.collection_detail[collectionName];
    }
  }
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

function isTelegramMessageDeleteNotFoundError(error: unknown) {
  const err = error as { code?: string; message?: string } | null | undefined;
  const code = err?.code ?? '';
  const message = (err?.message ?? '').toLowerCase();
  return code === 'TG_400' && message.includes('message to delete not found');
}

async function deleteTelegramMessages(args: {
  botToken: string;
  chatId: string;
  messageIds: number[];
  onError: (messageId: number, error: unknown) => void;
}) {
  for (const messageId of [...new Set(args.messageIds)].filter((item) => Number.isInteger(item) && item > 0)) {
    try {
      await sendTelegramRequest({
        botToken: args.botToken,
        method: 'deleteMessage',
        payload: {
          chat_id: args.chatId,
          message_id: messageId,
        },
      });
    } catch (error) {
      if (isTelegramMessageDeleteNotFoundError(error)) {
        continue;
      }
      args.onError(messageId, error);
    }
  }
}

// 删除合集导航状态消息
async function deleteCollectionNavStateMessages(args: {
  botToken: string;
  chatId: string;
  channelIdRaw: string;
  state: CollectionNavState;
}) {
  await deleteTelegramMessages({
    botToken: args.botToken,
    chatId: args.chatId,
    messageIds: [
      ...(args.state.indexMessageId ? [args.state.indexMessageId] : []),
      ...args.state.indexPageMessageIds,
    ],
    onError: (messageId, error) => {
      logError('[q_catalog] 删除旧合集索引分页消息失败', {
        channelId: args.channelIdRaw,
        staleMessageId: messageId,
        error,
      });
    },
  });

  for (const [collectionName, detailPages] of Object.entries(args.state.detailPageMessageIds)) {
    await deleteTelegramMessages({
      botToken: args.botToken,
      chatId: args.chatId,
      messageIds: [
        ...(args.state.detailMessageIds[collectionName] ? [args.state.detailMessageIds[collectionName]] : []),
        ...detailPages,
      ],
      onError: (messageId, error) => {
        logError('[q_catalog] 删除旧合集详情消息失败', {
          channelId: args.channelIdRaw,
          collectionName,
          staleMessageId: messageId,
          error,
        });
      },
    });
  }
}

// 构建目录索引内容
function buildCatalogIndexContent(args: {
  channelName: string;
  totalPages: number;
  collectionIndexLink?: string | null;
}) {
  return [
    `📚 ${args.channelName} 目录导航`,
    `共 ${args.totalPages} 页，点击下方按钮跳转。`,
    ...(args.collectionIndexLink
      ? [`<a href="${escapeHtml(args.collectionIndexLink)}">📚 合集索引</a>`]
      : []),
    `更新时间：${new Date(Date.now() + 8 * 3600 * 1000).toISOString().replace('T', ' ').slice(0, 19)}`,
  ].join('\n');
}

// 构建目录分页按钮
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

// 构建目录内容页回复标记（分页导航）
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

// 构建合集索引回复标记
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

// 构建合集详情回复标记
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

// 解析合集元数据
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

type CollectionCatalogConfig = {
  name: string;
  nameNormalized: string | null;
  navEnabled: boolean;
  navPageSize: number;
};

type CollectionCatalogEpisode = {
  collectionName: string;
  episodeNo: number;
  title: string;
  messageUrl: string;
  isMissingPlaceholder?: boolean;
};

// 从标题前缀中去掉“第N集”以避免模板重复拼接
function stripEpisodePrefixForTemplate(title: string, episodeNo: number) {
  const escapedEpisodeNo = String(episodeNo).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const punctClass = '[:：—－.、，,）)\\-]*';

  const patterns = [
    new RegExp(`^第\\s*0*${escapedEpisodeNo}\\s*[集话章回期]\\s*${punctClass}\\s*`, 'u'),
    new RegExp(`^0*${escapedEpisodeNo}\\s*[集话章回期]\\s*${punctClass}\\s*`, 'u'),
  ];

  let next = title.trim();
  for (const pattern of patterns) {
    next = next.replace(pattern, '').trim();
  }

  return next || title.trim();
}

// 格式化合集集标题
function formatCollectionEpisodeTitle(args: {
  episodeNo: number;
  episodeTitle?: string | null;
  sourceTitle?: string | null;
  templateText?: string | null;
}) {
  const safeTitle = (args.episodeTitle || '').trim();
  if (safeTitle) return safeTitle;

  const fallbackTitle = (args.sourceTitle || '').trim() || `第${args.episodeNo}集`;
  const safeTemplate = (args.templateText || '').trim();
  if (safeTemplate) {
    const dedupedTitle = stripEpisodePrefixForTemplate(fallbackTitle, args.episodeNo);
    return safeTemplate
      .replace(/\{episodeNo\}/g, String(args.episodeNo))
      .replace(/\{title\}/g, dedupedTitle);
  }

  return fallbackTitle;
}

// 构建合集集去重键
function buildCollectionEpisodeDedupKey(collectionName: string, episodeNo: number) {
  return `${normalizeCollectionKey(collectionName)}#${episodeNo}`;
}

interface CollectionCatalogReadProvider {
  getCollections(channelId: bigint): Promise<CollectionCatalogConfig[]>;
  getCollectionEpisodes(channelId: bigint): Promise<CollectionCatalogEpisode[]>;
}

class DbCollectionCatalogReadProvider implements CollectionCatalogReadProvider {
  // 获取合集列表
  async getCollections(channelId: bigint): Promise<CollectionCatalogConfig[]> {
    return prisma.collection.findMany({
      where: { channelId },
      select: { name: true, nameNormalized: true, navEnabled: true, navPageSize: true },
      orderBy: { id: 'asc' },
    });
  }

  // 获取合集集列表
  async getCollectionEpisodes(channelId: bigint): Promise<CollectionCatalogEpisode[]> {
    const collections = await prisma.collection.findMany({
      where: { channelId },
      select: {
        id: true,
        name: true,
        nameNormalized: true,
        templateText: true,
      },
      orderBy: { id: 'asc' },
    });

    const collectionNameById = new Map<string, string>();
    const collectionTemplateById = new Map<string, string | null>();
    const collectionNameByNormalized = new Map<string, string>();
    const collectionTemplateByNormalized = new Map<string, string | null>();
    for (const collection of collections) {
      const normalizedName = normalizeCollectionKey(collection.nameNormalized || collection.name);
      collectionNameById.set(collection.id.toString(), collection.name);
      collectionTemplateById.set(collection.id.toString(), collection.templateText ?? null);
      collectionNameByNormalized.set(normalizedName, collection.name);
      collectionTemplateByNormalized.set(normalizedName, collection.templateText ?? null);
    }

    const episodeRows = await prisma.collectionEpisode.findMany({
      where: {
        collection: {
          channelId,
        },
      },
      orderBy: [{ collectionId: 'asc' }, { episodeNo: 'asc' }],
      select: {
        collectionId: true,
        mediaAssetId: true,
        episodeNo: true,
        episodeTitle: true,
        telegramMessageLink: true,
        mediaAsset: {
          select: {
            originalName: true,
            sourceMeta: true,
          },
        },
      },
    });

    const mediaAssetIds = [...new Set(episodeRows.map((row) => row.mediaAssetId))];
    const dispatchLinkMap = new Map<string, string>();
    if (mediaAssetIds.length > 0) {
      const dispatchRows = await prisma.dispatchTask.findMany({
        where: {
          channelId,
          status: TaskStatus.success,
          telegramMessageLink: { not: null },
          mediaAssetId: { in: mediaAssetIds },
        },
        orderBy: [{ finishedAt: 'desc' }, { id: 'desc' }],
        select: {
          mediaAssetId: true,
          telegramMessageLink: true,
        },
      });

      for (const row of dispatchRows) {
        const key = row.mediaAssetId.toString();
        if (!dispatchLinkMap.has(key) && row.telegramMessageLink) {
          dispatchLinkMap.set(key, row.telegramMessageLink);
        }
      }
    }

    const collectionEpisodes = new Map<string, CollectionCatalogEpisode>();
    for (const row of episodeRows) {
      const collectionIdKey = row.collectionId.toString();
      const collectionName = collectionNameById.get(collectionIdKey);
      if (!collectionName) continue;

      const sourceTitle = getFileStem(row.mediaAsset?.originalName || '') || `第${row.episodeNo}集`;
      const title = formatCollectionEpisodeTitle({
        episodeNo: row.episodeNo,
        episodeTitle: row.episodeTitle,
        sourceTitle,
        templateText: collectionTemplateById.get(collectionIdKey) ?? null,
      });
      const messageUrl = (row.telegramMessageLink || dispatchLinkMap.get(row.mediaAssetId.toString()) || '').trim();
      collectionEpisodes.set(buildCollectionEpisodeDedupKey(collectionName, row.episodeNo), {
        collectionName,
        episodeNo: row.episodeNo,
        title,
        messageUrl,
      });
    }

    const batchSize = TYPEC_COLLECTION_FULL_SCAN_BATCH_SIZE;
    let cursorId: bigint | null = null;
    while (true) {
      const rows: Array<{
        id: bigint;
        telegramMessageLink: string | null;
        mediaAsset: { originalName: string | null; sourceMeta: unknown } | null;
      }> = await prisma.dispatchTask.findMany({
        where: {
          channelId,
          status: TaskStatus.success,
          telegramMessageId: { not: null },
        },
        orderBy: { id: 'asc' },
        take: batchSize,
        ...(cursorId ? { cursor: { id: cursorId }, skip: 1 } : {}),
        select: {
          id: true,
          telegramMessageLink: true,
          mediaAsset: {
            select: {
              originalName: true,
              sourceMeta: true,
            },
          },
        },
      });

      if (rows.length === 0) break;

      for (const row of rows) {
        const meta = parseCollectionMeta(row.mediaAsset?.sourceMeta);
        if (!meta) continue;

        const normalizedName = normalizeCollectionKey(meta.collectionName);
        const collectionName = collectionNameByNormalized.get(normalizedName) ?? meta.collectionName;
        const key = buildCollectionEpisodeDedupKey(collectionName, meta.episodeNo);
        if (collectionEpisodes.has(key)) continue;

        const sourceTitle = getFileStem(row.mediaAsset?.originalName || '') || `第${meta.episodeNo}集`;

        collectionEpisodes.set(key, {
          collectionName,
          episodeNo: meta.episodeNo,
          title: formatCollectionEpisodeTitle({
            episodeNo: meta.episodeNo,
            sourceTitle,
            templateText: collectionTemplateByNormalized.get(normalizedName) ?? null,
          }),
          messageUrl: row.telegramMessageLink || '',
        });
      }

      cursorId = rows[rows.length - 1].id;
      if (rows.length < batchSize) break;
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
      if (!meta) continue;

      const normalizedName = normalizeCollectionKey(meta.collectionName);
      const collectionName = collectionNameByNormalized.get(normalizedName) ?? meta.collectionName;
      const key = buildCollectionEpisodeDedupKey(collectionName, meta.episodeNo);
      const existing = collectionEpisodes.get(key);
      if (!existing) {
        collectionEpisodes.set(key, {
          collectionName,
          episodeNo: meta.episodeNo,
          title: `第${meta.episodeNo}集（暂缺）`,
          messageUrl: '',
          isMissingPlaceholder: true,
        });
      }
    }

    return [...collectionEpisodes.values()].sort((a, b) => {
      const collectionCompare = normalizeCollectionKey(a.collectionName).localeCompare(
        normalizeCollectionKey(b.collectionName),
        'zh-CN',
      );
      if (collectionCompare !== 0) return collectionCompare;
      return a.episodeNo - b.episodeNo;
    });
  }
}

class CachedCollectionCatalogReadProvider implements CollectionCatalogReadProvider {
  // ?????????????????????????
  constructor(private readonly dbProvider: CollectionCatalogReadProvider) { }

  // ?? get Collections ?????????????????????
  async getCollections(channelId: bigint): Promise<CollectionCatalogConfig[]> {
    return this.dbProvider.getCollections(channelId);
  }

  // ?? get Collection Episodes ?????????????????????
  async getCollectionEpisodes(channelId: bigint): Promise<CollectionCatalogEpisode[]> {
    const now = Date.now();
    const staleMs = TYPEC_COLLECTION_CACHE_STALE_SECONDS * 1000;
    const snapshots = await prisma.collectionEpisodeSnapshot.findMany({
      where: { channelId },
      select: {
        collectionNameNormalized: true,
        episodeNo: true,
        telegramMessageUrl: true,
        title: true,
        isMissingPlaceholder: true,
        snapshotUpdatedAt: true,
      },
      orderBy: [
        { collectionNameNormalized: 'asc' },
        { episodeNo: 'asc' },
      ],
    });

    const snapshotHeads = await prisma.collectionSnapshot.findMany({
      where: { channelId, isDeleted: false },
      select: {
        collectionName: true,
        collectionNameNormalized: true,
        lastRebuildAt: true,
      },
    });

    const staleCount = snapshotHeads.filter(
      (item) => now - item.lastRebuildAt.getTime() > staleMs,
    ).length;

    const snapshotNameMap = new Map(
      snapshotHeads.map((item) => [normalizeCollectionKey(item.collectionNameNormalized), item.collectionName]),
    );

    const episodes = snapshots.map((item) => {
      const key = normalizeCollectionKey(item.collectionNameNormalized);
      return {
        collectionName: snapshotNameMap.get(key) ?? item.collectionNameNormalized,
        episodeNo: item.episodeNo,
        title: item.title ?? '',
        messageUrl: item.telegramMessageUrl || '',
        isMissingPlaceholder: item.isMissingPlaceholder,
      } as CollectionCatalogEpisode;
    });

    logger.info('[q_catalog] 缓存读模型命中统计', {
      channelId: channelId.toString(),
      cache_episode_rows: snapshots.length,
      cache_collection_rows: snapshotHeads.length,
      cache_stale_count: staleCount,
      cache_stale_threshold_seconds: TYPEC_COLLECTION_CACHE_STALE_SECONDS,
    });

    if (snapshots.length === 0 && TYPEC_COLLECTION_CACHE_FALLBACK_TO_DB) {
      logger.info('[q_catalog] 缓存为空，回源DB读取合集详情', {
        channelId: channelId.toString(),
      });
      return this.dbProvider.getCollectionEpisodes(channelId);
    }

    if (staleCount > 0 && TYPEC_COLLECTION_CACHE_FALLBACK_TO_DB) {
      logger.info('[q_catalog] 缓存存在过期数据，回源DB读取合集详情', {
        channelId: channelId.toString(),
        staleCount,
      });
      return this.dbProvider.getCollectionEpisodes(channelId);
    }

    return episodes;
  }
}

// ??????????????????????????????
export async function handleCatalogJob(
  channelIdRaw: string,
  options?: {
    selfHealOnly?: boolean;
    triggerType?: 'scheduler' | 'manual_repair';
    runId?: string;
    forceRepublish?: boolean;
  },
) {
  const runStartedAt = Date.now();
  const runId = options?.runId || randomUUID();
  const triggerType = options?.triggerType || 'scheduler';
  let catalogTaskId: bigint | null = null;
  const channelId = BigInt(channelIdRaw);

  catalogMetrics.publishRunTotal += 1;
  if (triggerType === 'manual_repair') {
    catalogMetrics.publishRunManualRepairTotal += 1;
  } else {
    catalogMetrics.publishRunSchedulerTotal += 1;
  }

  logger.info('[q_catalog] 目录发布开始', {
    runId,
    channelId: channelIdRaw,
    triggerType,
    selfHealOnly: Boolean(options?.selfHealOnly),
    dataSource: TYPEC_READ_FROM_CATALOG_SOURCE ? 'catalog_source_item' : 'dispatch_task',
  });

  const channelLock = await tryAcquireChannelLock({ scope: 'catalog', channelId });
  if (!channelLock.acquired) {
    catalogMetrics.publishRunSkippedTotal += 1;
    logger.info('[q_catalog] 跳过执行：同频道已有进行中的目录任务', {
      runId,
      channelId: channelIdRaw,
      triggerType,
      lockKey: channelLock.lockKey,
      reason: 'catalog_channel_lock_not_acquired',
    });
    return { ok: true, skipped: true, reason: 'catalog_channel_lock_not_acquired' };
  }

  try {
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
      catalogMetrics.publishRunSkippedTotal += 1;
      logger.info('[q_catalog] 跳过执行：频道未启用目录或未激活', {
        runId,
        channelId: channelIdRaw,
        triggerType,
        reason: 'channel_nav_disabled_or_inactive',
      });
      return { ok: true, skipped: true, reason: '导航未启用或频道未激活' };
    }
    if (!channel.navTemplateText || !channel.defaultBot) {
      throw new Error(`频道缺少导航模板或默认机器人: ${channelIdRaw}`);
    }
    if (channel.defaultBot.status !== 'active') {
      throw new Error(`频道机器人未启用: ${channelIdRaw}`);
    }

    if (CATALOG_CHANNEL_INTERVAL_GUARD_ENABLED && !options?.selfHealOnly) {
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

      // 当过了间隔检查期后，进一步判断距离上次有效更新是否有产生新的视频记录
      // 注意：需同时感知普通视频（catalogSourceItem）和合集视频（collectionEpisode）两种更新来源
      if (channel.lastNavUpdateAt && triggerType !== 'manual_repair') {
        let hasNewItem: { id: unknown } | null = null;
        let newItemSource: 'catalog_source_item' | 'collection_episode' | 'dispatch_task' | null = null;

        if (TYPEC_READ_FROM_CATALOG_SOURCE) {
          // 1. 优先查普通视频/图集（catalogSourceItem）
          const newNormalItem = await (prisma as any).catalogSourceItem.findFirst({
            where: {
              channelId,
              publishedAt: { gt: channel.lastNavUpdateAt },
            },
            select: { id: true },
          });

          if (newNormalItem) {
            hasNewItem = newNormalItem;
            newItemSource = 'catalog_source_item';
          } else {
            // 2. 追加查询：是否有合集集数在上次目录更新后被更新过
            //    collectionEpisode.updatedAt 在 Type B 分发成功写入 telegramMessageLink 后会变更
            const newCollectionEpisode = await prisma.collectionEpisode.findFirst({
              where: {
                collection: { channelId },
                updatedAt: { gt: channel.lastNavUpdateAt },
              },
              select: { id: true },
            });

            if (newCollectionEpisode) {
              hasNewItem = newCollectionEpisode;
              newItemSource = 'collection_episode';
            }
          }
        } else {
          // fallback：直接查 dispatchTask（TYPEC_READ_FROM_CATALOG_SOURCE=false 时）
          const newTask = await prisma.dispatchTask.findFirst({
            where: {
              channelId,
              status: TaskStatus.success,
              telegramMessageId: { not: null },
              finishedAt: { gt: channel.lastNavUpdateAt },
            },
            select: { id: true },
          });

          if (newTask) {
            hasNewItem = newTask;
            newItemSource = 'dispatch_task';
          }
        }

        if (!hasNewItem) {
          catalogMetrics.publishRunSkippedTotal += 1;
          logger.info('[q_catalog] 跳过执行：距离上次更新没有产生新视频记录', {
            runId,
            channelId: channelIdRaw,
            triggerType,
            reason: 'no_new_records_since_last_update',
            checkedSources: TYPEC_READ_FROM_CATALOG_SOURCE
              ? ['catalog_source_item', 'collection_episode']
              : ['dispatch_task'],
          });

          // 更新 lastNavUpdateAt，让频道重新进入下一个间隔等待，防止在接下来的几十分钟内每秒查一遍库
          await prisma.channel.update({
            where: { id: channelId },
            data: { lastNavUpdateAt: guardNow },
          });

          return { ok: true, skipped: true, reason: '没有新视频产生，跳过更新' };
        }

        logger.info('[q_catalog] 新内容检测通过，继续执行目录更新', {
          runId,
          channelId: channelIdRaw,
          triggerType,
          newItemSource,
        });
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
    const channelAny = channel as any;
    const collectionNavEnabled =
      typeof channelAny.collection_nav_enabled === 'boolean' ? channelAny.collection_nav_enabled : true;

    const videos = TYPEC_READ_FROM_CATALOG_SOURCE
      ? await (async () => {
        const sourceRows = await (prisma as any).catalogSourceItem.findMany({
          where: { channelId },
          orderBy: [{ publishedAt: 'desc' }],
          take: recentLimit,
          select: {
            title: true,
            caption: true,
            telegramMessageLink: true,
          },
        });

        if (sourceRows.length === 0) {
          if (catalogTaskId) {
            await prisma.catalogTask.update({
              where: { id: catalogTaskId },
              data: {
                status: CatalogTaskStatus.cancelled,
                finishedAt: new Date(),
                errorMessage: '没有可用的目录投影记录',
              },
            });
          }
          return [] as CatalogVideo[];
        }

        const orderedSourceRows = [...sourceRows].reverse();

        return orderedSourceRows
          .filter((row: any) => Boolean(row.telegramMessageLink))
          .map((row: any) => {
            const fallbackTitle = '未命名视频';
            const safeCaption = isAiFailureText(row.caption) ? fallbackTitle : (row.caption || '').trim();
            const shortTitle = (row.title || buildCatalogShortTitle(safeCaption, fallbackTitle) || fallbackTitle).trim();
            return {
              message_url: row.telegramMessageLink || '',
              short_title: shortTitle,
            };
          });
      })()
      : await (async () => {
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
          return [] as CatalogVideo[];
        }

        const orderedDispatchTasks = [...dispatchTasks].reverse();
        const regularDispatchTasks = collectionNavEnabled
          ? orderedDispatchTasks.filter((task) => !parseCollectionMeta(task.mediaAsset?.sourceMeta))
          : orderedDispatchTasks;

        return regularDispatchTasks.map((t) => {
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
      })();

    if (videos.length === 0) {
      // 普通视频列表为空时，不立即 return。
      // 频道可能是纯合集频道（所有视频均为合集，catalogSourceItem 为空），合集目录仍需渲染。
      // 此处仅记录日志，继续向下执行合集渲染逻辑；
      // 若合集数据也为空，将在后续的合集渲染阶段各自跳过，最终由 hash 门控决定是否发布空目录。
      logger.info('[q_catalog] 普通视频列表为空，检查是否有合集内容需要渲染', {
        runId,
        channelId: channelIdRaw,
        triggerType,
        dataSource: TYPEC_READ_FROM_CATALOG_SOURCE ? 'catalog_source_item' : 'dispatch_task',
      });
    }

    catalogMetrics.publishEmptyRunConsecutive = 0;

    const dbCollectionReadProvider: CollectionCatalogReadProvider = new DbCollectionCatalogReadProvider();
    const collectionReadProvider: CollectionCatalogReadProvider =
      TYPEC_COLLECTION_DATA_SOURCE === 'cache'
        ? new CachedCollectionCatalogReadProvider(dbCollectionReadProvider)
        : dbCollectionReadProvider;

    logger.info('[q_catalog] 合集数据源选择', {
      channelId: channelIdRaw,
      requestedDataSource: TYPEC_COLLECTION_DATA_SOURCE,
      actualProvider: TYPEC_COLLECTION_DATA_SOURCE === 'cache' ? 'cache' : 'db',
      cacheFallbackToDb: TYPEC_COLLECTION_CACHE_FALLBACK_TO_DB,
    });

    const collectionConfigsAll = await collectionReadProvider.getCollections(channelId);
    const collectionEpisodesAll = await collectionReadProvider.getCollectionEpisodes(channelId);

    const collectionGroups = new Map<string, CollectionCatalogEpisode[]>();
    for (const episode of collectionEpisodesAll) {
      const group = collectionGroups.get(episode.collectionName) ?? [];
      group.push(episode);
      collectionGroups.set(episode.collectionName, group);
    }

    const collectionNameMapByNormalized = new Map<string, string>();
    for (const name of collectionGroups.keys()) {
      const normalizedName = normalizeCollectionKey(name);
      if (!collectionNameMapByNormalized.has(normalizedName)) {
        collectionNameMapByNormalized.set(normalizedName, name);
      }
    }

    const enabledCollectionConfigs = collectionConfigsAll.filter((item) => item.navEnabled);

    const collectionIndexShowEmpty = TYPEC_COLLECTION_INDEX_SHOW_EMPTY;

    const filteredCollections = collectionConfigsAll
      .map((item) => {
        if (!item.navEnabled) {
          return { name: item.name, reason: 'not_enabled' as const };
        }
        const normalizedName = normalizeCollectionKey(item.nameNormalized || item.name);
        if (!collectionIndexShowEmpty && !collectionNameMapByNormalized.has(normalizedName)) {
          return { name: item.name, reason: 'no_data' as const };
        }
        return null;
      })
      .filter((item): item is { name: string; reason: 'not_enabled' | 'no_data' } => Boolean(item));

    const collectionConfigs = enabledCollectionConfigs.filter((item) => {
      const normalizedName = normalizeCollectionKey(item.nameNormalized || item.name);
      const hasEpisodes = collectionNameMapByNormalized.has(normalizedName);
      return collectionIndexShowEmpty || hasEpisodes;
    });

    logger.info('[q_catalog] 合集读模型统计', {
      channelId: channelIdRaw,
      dataSource: 'full',
      collection_config_total: collectionConfigsAll.length,
      collection_config_enabled_total: enabledCollectionConfigs.length,
      collection_episode_total: collectionEpisodesAll.length,
      collection_index_show_empty: collectionIndexShowEmpty,
      collection_index_rendered_total: collectionConfigs.length,
      empty_collections: collectionConfigs
        .filter((item) => !collectionNameMapByNormalized.has(normalizeCollectionKey(item.nameNormalized || item.name)))
        .map((item) => item.name),
      filtered_collections: filteredCollections,
    });

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
    const selfHealEnabledOnRun = TYPEC_SELF_HEAL_ON_RUN;
    let selfHealFixedCount = 0;
    let selfHealFallbackCount = 0;
    let selfHealOrphanCleanedCount = 0;
    let hashGateSkipCount = 0;
    let hashGatePublishCount = 0;

    const collectionSections: string[] = [];
    let collectionIndexLinkInMainCatalog: string | null = null;
    let nextCollectionNavState: CollectionNavState | null = null;
    const existingNavState = parseCollectionNavState(channel.navReplyMarkup);
    const existingHashState = parseCatalogHashState(channel.navReplyMarkup);
    const nextHashState: CatalogHashState = JSON.parse(JSON.stringify(existingHashState || {}));
    const hashSchemaVersion = Math.max(1, Number(TYPEC_HASH_SCHEMA_VERSION || 1));
    const hashForceRepublish = Boolean(options?.forceRepublish) || triggerType === 'manual_repair';
    if (collectionConfigs.length === 0) {
      await deleteCollectionNavStateMessages({
        botToken,
        chatId: channel.tgChatId,
        channelIdRaw,
        state: existingNavState,
      });
    }
    if (collectionConfigs.length > 0) {
      const names = collectionConfigs.map((item) => item.name);
      const detailMessageIds: Record<string, number> = {};
      const detailPageMessageIds: Record<string, number[]> = {};

      const collectionIndexPageSize = Math.max(1, Math.min(50, Number(channelAny.collection_index_page_size ?? 20)));

      const collectionNameToNormalized = new Map(
        collectionConfigs.map((item) => [item.name, normalizeCollectionKey(item.nameNormalized || item.name)]),
      );
      const normalizedNames = [...new Set(collectionNameToNormalized.values())];

      logger.info('[q_catalog] 合集配置原始读取结果(按频道)', {
        channelId: channelIdRaw,
        fetchedCount: collectionConfigsAll.length,
        fetchedNames: collectionConfigsAll.map((item) => item.name),
      });

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
        const normalizedName = collectionNameToNormalized.get(name) ?? normalizeCollectionKey(name);
        const matchedGroupName = collectionNameMapByNormalized.get(normalizedName);
        const episodes = (matchedGroupName ? collectionGroups.get(matchedGroupName) ?? [] : []).sort(
          (a, b) => a.episodeNo - b.episodeNo,
        );
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
        const episodePages =
          episodes.length > 0
            ? chunkVideos(episodes, collectionDetailPageSize)
            : [[] as CollectionCatalogEpisode[]];
        const existingDetailPages = existingNavState.detailPageMessageIds[name] ?? [];
        const publishedDetailPages: number[] = [];
        const detailSecondPassRequired = new Set<number>();
        const detailOrphanCandidateIds = new Set<number>();

        const detailTexts: string[] = [];

        for (let pageIndex = 0; pageIndex < episodePages.length; pageIndex += 1) {
          const pageLines = episodePages[pageIndex].map((ep) => {
            const displayTitle = ep.title.trim();
            const safeTitle = escapeHtml(displayTitle || `第${ep.episodeNo}集`);

            if (ep.isMissingPlaceholder || !ep.messageUrl) {
              return safeTitle;
            }

            const safeUrl = escapeHtml(ep.messageUrl);
            return `<a href="${safeUrl}">${safeTitle}</a>`;
          });
          const detailBodyLines =
            pageLines.length > 0 ? pageLines : ['该合集暂无内容，后续更新后将自动补充。'];

          const detailText = [
            `📺 ${escapeHtml(name)}`,
            ...detailBodyLines,
            ...(episodePages.length > 1 ? [`—— 第${pageIndex + 1}/${episodePages.length}页 ——`] : []),
          ].join('\n');

          detailTexts.push(detailText);

          const existingDetailMessageId = existingDetailPages[pageIndex] ?? null;
          const firstPassHash = buildPageCombinedHash({
            text: detailText,
            replyMarkup: undefined,
            schemaVersion: hashSchemaVersion,
          });
          const firstPassOldHashRecord = readHashRecord(
            nextHashState,
            'collection_detail',
            pageIndex + 1,
            `${name}__firstpass`,
          );
          const shouldSkipFirstPass = shouldSkipByHash({
            enabled: TYPEC_HASH_GATE_ENABLED,
            forceRepublish: hashForceRepublish,
            existingMessageId: existingDetailMessageId,
            oldRecord: firstPassOldHashRecord,
            newCombinedHash: firstPassHash.combinedHash,
            schemaVersion: hashSchemaVersion,
          });

          if (shouldSkipFirstPass && existingDetailMessageId) {
            hashGateSkipCount += 1;
            publishedDetailPages.push(existingDetailMessageId);
          } else {
            hashGatePublishCount += 1;
            const detailPublishResult = await publishCatalogMessage({
              botToken,
              chatId: channel.tgChatId,
              text: detailText,
              existingMessageId: existingDetailMessageId,
            });

            if (selfHealEnabledOnRun && detailPublishResult.notModified) selfHealFixedCount += 1;
            if (selfHealEnabledOnRun && detailPublishResult.fallbackSent) selfHealFallbackCount += 1;

            publishedDetailPages.push(detailPublishResult.messageId);
            if (detailPublishResult.isNewMessage || detailPublishResult.messageId !== existingDetailMessageId) {
              detailSecondPassRequired.add(pageIndex + 1);
            }
            writeHashRecord(nextHashState, {
              scope: 'collection_detail',
              collectionName: `${name}__firstpass`,
              pageNo: pageIndex + 1,
              combinedHash: firstPassHash.combinedHash,
              schemaVersion: hashSchemaVersion,
            });
          }
        }

        if (episodePages.length > 1) {
          for (let pageIndex = 0; pageIndex < episodePages.length; pageIndex += 1) {
            const currentMessageId = publishedDetailPages[pageIndex] ?? null;
            if (!currentMessageId) continue;

            const detailReplyMarkup = buildCollectionDetailReplyMarkup({
              chatId: channel.tgChatId,
              currentPage: pageIndex + 1,
              totalPages: episodePages.length,
              detailPageMessageIds: publishedDetailPages,
            }) ?? undefined;
            const detailSecondPassHash = buildPageCombinedHash({
              text: detailTexts[pageIndex],
              replyMarkup: detailReplyMarkup,
              schemaVersion: hashSchemaVersion,
            });
            const detailSecondPassOldHash = readHashRecord(nextHashState, 'collection_detail', pageIndex + 1, name);
          const shouldSkipDetailSecondPass = shouldSkipByHash({
            enabled: TYPEC_HASH_GATE_ENABLED,
            forceRepublish: hashForceRepublish,
            existingMessageId: currentMessageId,
            oldRecord: detailSecondPassOldHash,
            newCombinedHash: detailSecondPassHash.combinedHash,
            schemaVersion: hashSchemaVersion,
          }) && !detailSecondPassRequired.has(pageIndex + 1);

            if (shouldSkipDetailSecondPass) {
              hashGateSkipCount += 1;
              continue;
            }

            hashGatePublishCount += 1;
            const detailRepublishResult = await publishCatalogMessage({
              botToken,
              chatId: channel.tgChatId,
              text: detailTexts[pageIndex],
              existingMessageId: currentMessageId,
              replyMarkup: detailReplyMarkup,
            });

            if (selfHealEnabledOnRun && detailRepublishResult.notModified) selfHealFixedCount += 1;
            if (selfHealEnabledOnRun && detailRepublishResult.fallbackSent) selfHealFallbackCount += 1;

            applyPublishResultAndRewriteId({
              pageIds: publishedDetailPages,
              pageIndex,
              resultMessageId: detailRepublishResult.messageId,
              orphanCandidateIds: detailOrphanCandidateIds,
              scope: 'collection_detail',
              channelIdRaw,
            });

            writeHashRecord(nextHashState, {
              scope: 'collection_detail',
              collectionName: name,
              pageNo: pageIndex + 1,
              combinedHash: detailSecondPassHash.combinedHash,
              schemaVersion: hashSchemaVersion,
            });
          }
        }

        const reconciledDetailPageIds = reconcilePageMessageIds({
          ids: publishedDetailPages,
          expectedCount: episodePages.length,
          scope: `collection_detail:${name}`,
          channelIdRaw,
        });
        publishedDetailPages.length = 0;
        publishedDetailPages.push(...reconciledDetailPageIds);

        const staleDetailPages = [
          ...existingDetailPages.slice(publishedDetailPages.length),
          ...[...detailOrphanCandidateIds].filter((id) => !publishedDetailPages.includes(id)),
        ];
        if (selfHealEnabledOnRun) selfHealOrphanCleanedCount += staleDetailPages.length;
        await deleteTelegramMessages({
          botToken,
          chatId: channel.tgChatId,
          messageIds: staleDetailPages,
          onError: (messageId, error) => {
            logError('[q_catalog] 删除旧合集详情分页消息失败', {
              channelId: channelIdRaw,
              collectionName: name,
              staleMessageId: messageId,
              error,
            });
          },
        });

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
      if (indexPages.length === 0) {
        await deleteCollectionNavStateMessages({
          botToken,
          chatId: channel.tgChatId,
          channelIdRaw,
          state: existingNavState,
        });
      } else {
        const existingIndexPages = existingNavState.indexPageMessageIds;
        const publishedIndexPages: number[] = [];
        const indexSecondPassRequired = new Set<number>();
        const indexOrphanCandidateIds = new Set<number>();

        for (let pageIndex = 0; pageIndex < indexPages.length; pageIndex += 1) {
          const text = [
            '📚 合集索引',
            '请选择合集：',
            ...(indexPages.length > 1 ? [`—— 第${pageIndex + 1}/${indexPages.length}页 ——`] : []),
          ].join('\n');

          const firstPassReplyMarkup = {
            inline_keyboard: indexPages[pageIndex].map((item) => [{ text: item.text, url: item.url }]),
          };

          const existingIndexMessageId = existingIndexPages[pageIndex] ?? null;
          const indexFirstPassHash = buildPageCombinedHash({
            text,
            replyMarkup: firstPassReplyMarkup,
            schemaVersion: hashSchemaVersion,
          });
          const indexFirstPassOldHash = readHashRecord(
            nextHashState,
            'collection_index',
            pageIndex + 1,
            '__firstpass',
          );
          const shouldSkipIndexFirstPass = shouldSkipByHash({
            enabled: TYPEC_HASH_GATE_ENABLED,
            forceRepublish: hashForceRepublish,
            existingMessageId: existingIndexMessageId,
            oldRecord: indexFirstPassOldHash,
            newCombinedHash: indexFirstPassHash.combinedHash,
            schemaVersion: hashSchemaVersion,
          });

          if (shouldSkipIndexFirstPass && existingIndexMessageId) {
            hashGateSkipCount += 1;
            publishedIndexPages.push(existingIndexMessageId);
          } else {
            hashGatePublishCount += 1;
            const indexPublishResult = await publishCatalogMessage({
              botToken,
              chatId: channel.tgChatId,
              text,
              existingMessageId: existingIndexMessageId,
              replyMarkup: firstPassReplyMarkup,
            });

            if (selfHealEnabledOnRun && indexPublishResult.notModified) selfHealFixedCount += 1;
            if (selfHealEnabledOnRun && indexPublishResult.fallbackSent) selfHealFallbackCount += 1;

            publishedIndexPages.push(indexPublishResult.messageId);
            if (indexPublishResult.isNewMessage || indexPublishResult.messageId !== existingIndexMessageId) {
              indexSecondPassRequired.add(pageIndex + 1);
            }
            writeHashRecord(nextHashState, {
              scope: 'collection_index',
              pageNo: pageIndex + 1,
              combinedHash: indexFirstPassHash.combinedHash,
              schemaVersion: hashSchemaVersion,
            });
          }
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
          const indexSecondPassHash = buildPageCombinedHash({
            text,
            replyMarkup,
            schemaVersion: hashSchemaVersion,
          });
          const indexSecondPassOldHash = readHashRecord(nextHashState, 'collection_index', pageIndex + 1);
          const shouldSkipIndexSecondPass = shouldSkipByHash({
            enabled: TYPEC_HASH_GATE_ENABLED,
            forceRepublish: hashForceRepublish,
            existingMessageId: currentMessageId,
            oldRecord: indexSecondPassOldHash,
            newCombinedHash: indexSecondPassHash.combinedHash,
            schemaVersion: hashSchemaVersion,
          }) && !indexSecondPassRequired.has(pageIndex + 1);

          if (shouldSkipIndexSecondPass) {
            hashGateSkipCount += 1;
            continue;
          }

          hashGatePublishCount += 1;
          const indexRepublishResult = await publishCatalogMessage({
            botToken,
            chatId: channel.tgChatId,
            text,
            existingMessageId: currentMessageId,
            replyMarkup,
          });

          if (selfHealEnabledOnRun && indexRepublishResult.notModified) selfHealFixedCount += 1;
          if (selfHealEnabledOnRun && indexRepublishResult.fallbackSent) selfHealFallbackCount += 1;

          applyPublishResultAndRewriteId({
            pageIds: publishedIndexPages,
            pageIndex,
            resultMessageId: indexRepublishResult.messageId,
            orphanCandidateIds: indexOrphanCandidateIds,
            scope: 'collection_index',
            channelIdRaw,
          });

          writeHashRecord(nextHashState, {
            scope: 'collection_index',
            pageNo: pageIndex + 1,
            combinedHash: indexSecondPassHash.combinedHash,
            schemaVersion: hashSchemaVersion,
          });
        }

        const reconciledIndexPageIds = reconcilePageMessageIds({
          ids: publishedIndexPages,
          expectedCount: indexPages.length,
          scope: 'collection_index',
          channelIdRaw,
        });
        publishedIndexPages.length = 0;
        publishedIndexPages.push(...reconciledIndexPageIds);

        const staleIndexPages = [
          ...existingIndexPages.slice(publishedIndexPages.length),
          ...[...indexOrphanCandidateIds].filter((id) => !publishedIndexPages.includes(id)),
        ];
        if (selfHealEnabledOnRun) selfHealOrphanCleanedCount += staleIndexPages.length;
        await deleteTelegramMessages({
          botToken,
          chatId: channel.tgChatId,
          messageIds: staleIndexPages,
          onError: (messageId, error) => {
            logError('[q_catalog] 删除旧合集索引分页消息失败', {
              channelId: channelIdRaw,
              staleMessageId: messageId,
              error,
            });
          },
        });

        const indexMessageId = publishedIndexPages[0] ?? null;
        collectionIndexLinkInMainCatalog = indexMessageId
          ? toTelegramMessageLink(channel.tgChatId, indexMessageId)
          : null;

        const staleCollectionNames = Object.keys(existingNavState.detailPageMessageIds).filter(
          (name) => !Object.prototype.hasOwnProperty.call(detailPageMessageIds, name),
        );
        for (const staleName of staleCollectionNames) {
          const stalePages = existingNavState.detailPageMessageIds[staleName] ?? [];
          if (selfHealEnabledOnRun) selfHealOrphanCleanedCount += stalePages.length;
          await deleteTelegramMessages({
            botToken,
            chatId: channel.tgChatId,
            messageIds: stalePages,
            onError: (messageId, error) => {
              logError('[q_catalog] 删除旧合集详情消息失败', {
                channelId: channelIdRaw,
                collectionName: staleName,
                staleMessageId: messageId,
                error,
              });
            },
          });
        }

        nextCollectionNavState = {
          indexMessageId,
          indexPageMessageIds: publishedIndexPages,
          detailMessageIds,
          detailPageMessageIds,
        };
      }
    }

    const mainCatalogPageContents = pageContents.map((pageText, index) => {
      if (index !== 0 || !collectionNavEnabled || !collectionIndexLinkInMainCatalog) return pageText;
      const base = pageText;
      const safeLink = escapeHtml(collectionIndexLinkInMainCatalog);
      return `${base}\n\n<a href="${safeLink}">📚 合集索引</a>`;
    });

    const mainCatalogContent = mainCatalogPageContents.join('\n\n');

    const content = [mainCatalogContent, ...collectionSections].filter(Boolean).join('\n\n');
    const contentPreview = content.slice(0, 4000);
    const hasMainCatalogContent = mainCatalogContent.trim().length > 0;
    let pinAttempted = false;
    let pinSuccess: boolean | null = null;
    let pinErrorMessage: string | null = null;

    try {
      const channelAny = channel as any;
      const storedPageMessageIds = parseStoredPageMessageIds(channelAny.navPageMessageIds);
      const publishedPageMessageIds: number[] = [];
      const mainSecondPassRequired = new Set<number>();
      const mainPageOrphanCandidateIds = new Set<number>();
      const existingMainNavMessageId = channel.navMessageId ? Number(channel.navMessageId) : null;

      if (!hasMainCatalogContent) {
        await deleteTelegramMessages({
          botToken,
          chatId: channel.tgChatId,
          messageIds: [
            ...(existingMainNavMessageId ? [existingMainNavMessageId] : []),
            ...storedPageMessageIds,
          ],
          onError: (messageId, error) => {
            logError('[q_catalog] 删除旧频道导航消息失败', {
              channelId: channelIdRaw,
              staleMessageId: messageId,
              error,
            });
          },
        });
      } else if (pageContents.length <= 1) {
        const publishResult = await publishCatalogMessage({
          botToken,
          chatId: channel.tgChatId,
          text: mainCatalogPageContents[0] ?? mainCatalogContent,
          existingMessageId: existingMainNavMessageId,
          clearReplyMarkup: true,
        });

        if (selfHealEnabledOnRun && publishResult.notModified) selfHealFixedCount += 1;
        if (selfHealEnabledOnRun && publishResult.fallbackSent) selfHealFallbackCount += 1;

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
          const pageText = mainCatalogPageContents[pageIndex] ?? pageContents[pageIndex];
          const existingMainPageId = storedPageMessageIds[pageIndex] ?? null;
          const mainFirstPassHash = buildPageCombinedHash({
            text: pageText,
            replyMarkup: undefined,
            schemaVersion: hashSchemaVersion,
          });
          const mainFirstPassOldHash = readHashRecord(nextHashState, 'main_catalog', pageIndex + 1, '__firstpass');
          const shouldSkipMainFirstPass = shouldSkipByHash({
            enabled: TYPEC_HASH_GATE_ENABLED,
            forceRepublish: hashForceRepublish,
            existingMessageId: existingMainPageId,
            oldRecord: mainFirstPassOldHash,
            newCombinedHash: mainFirstPassHash.combinedHash,
            schemaVersion: hashSchemaVersion,
          });

          if (shouldSkipMainFirstPass && existingMainPageId) {
            hashGateSkipCount += 1;
            publishedPageMessageIds.push(existingMainPageId);
            continue;
          }

          hashGatePublishCount += 1;
          const publishResult = await publishCatalogMessage({
            botToken,
            chatId: channel.tgChatId,
            text: pageText,
            existingMessageId: existingMainPageId,
          });
          if (selfHealEnabledOnRun && publishResult.notModified) selfHealFixedCount += 1;
          if (selfHealEnabledOnRun && publishResult.fallbackSent) selfHealFallbackCount += 1;
          publishedPageMessageIds.push(publishResult.messageId);
          if (publishResult.isNewMessage || publishResult.messageId !== existingMainPageId) {
            mainSecondPassRequired.add(pageIndex + 1);
          }
          writeHashRecord(nextHashState, {
            scope: 'main_catalog',
            pageNo: pageIndex + 1,
            combinedHash: mainFirstPassHash.combinedHash,
            schemaVersion: hashSchemaVersion,
          });
        }

        for (let pageIndex = 0; pageIndex < pageContents.length; pageIndex += 1) {
          const pageText = mainCatalogPageContents[pageIndex] ?? pageContents[pageIndex];
          const mainSecondPassReplyMarkup = buildCatalogContentPageReplyMarkup({
            chatId: channel.tgChatId,
            currentPage: pageIndex + 1,
            totalPages: pageContents.length,
            pageMessageIds: publishedPageMessageIds,
          }) ?? undefined;
          const mainSecondPassHash = buildPageCombinedHash({
            text: pageText,
            replyMarkup: mainSecondPassReplyMarkup,
            schemaVersion: hashSchemaVersion,
          });
          const mainSecondPassOldHash = readHashRecord(nextHashState, 'main_catalog', pageIndex + 1);
          const shouldSkipMainSecondPass = shouldSkipByHash({
            enabled: TYPEC_HASH_GATE_ENABLED,
            forceRepublish: hashForceRepublish,
            existingMessageId: publishedPageMessageIds[pageIndex],
            oldRecord: mainSecondPassOldHash,
            newCombinedHash: mainSecondPassHash.combinedHash,
            schemaVersion: hashSchemaVersion,
          }) && !mainSecondPassRequired.has(pageIndex + 1);

          if (shouldSkipMainSecondPass) {
            hashGateSkipCount += 1;
            continue;
          }

          hashGatePublishCount += 1;
          const republishResult = await publishCatalogMessage({
            botToken,
            chatId: channel.tgChatId,
            text: pageText,
            existingMessageId: publishedPageMessageIds[pageIndex],
            replyMarkup: mainSecondPassReplyMarkup,
          });

          if (selfHealEnabledOnRun && republishResult.notModified) selfHealFixedCount += 1;
          if (selfHealEnabledOnRun && republishResult.fallbackSent) selfHealFallbackCount += 1;

          applyPublishResultAndRewriteId({
            pageIds: publishedPageMessageIds,
            pageIndex,
            resultMessageId: republishResult.messageId,
            orphanCandidateIds: mainPageOrphanCandidateIds,
            scope: 'main_catalog',
            channelIdRaw,
          });

          writeHashRecord(nextHashState, {
            scope: 'main_catalog',
            pageNo: pageIndex + 1,
            combinedHash: mainSecondPassHash.combinedHash,
            schemaVersion: hashSchemaVersion,
          });
        }

        const reconciledMainPageIds = reconcilePageMessageIds({
          ids: publishedPageMessageIds,
          expectedCount: pageContents.length,
          scope: 'main_catalog',
          channelIdRaw,
        });
        publishedPageMessageIds.length = 0;
        publishedPageMessageIds.push(...reconciledMainPageIds);

        const indexReplyMarkup = buildCatalogPageButtons({
          chatId: channel.tgChatId,
          pageMessageIds: publishedPageMessageIds,
        });

        const indexPublishResult = await publishCatalogMessage({
          botToken,
          chatId: channel.tgChatId,
          text: buildCatalogIndexContent({
            channelName: channel.name,
            totalPages: pageContents.length,
            collectionIndexLink: collectionIndexLinkInMainCatalog,
          }),
          existingMessageId: existingMainNavMessageId,
          replyMarkup: indexReplyMarkup,
        });

        if (selfHealEnabledOnRun && indexPublishResult.notModified) selfHealFixedCount += 1;
        if (selfHealEnabledOnRun && indexPublishResult.fallbackSent) selfHealFallbackCount += 1;

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

      const stalePageMessageIds = hasMainCatalogContent
        ? [
          ...storedPageMessageIds.slice(publishedPageMessageIds.length),
          ...[...mainPageOrphanCandidateIds].filter((id) => !publishedPageMessageIds.includes(id)),
        ]
        : [];
      if (selfHealEnabledOnRun) selfHealOrphanCleanedCount += stalePageMessageIds.length;
      await deleteTelegramMessages({
        botToken,
        chatId: channel.tgChatId,
        messageIds: stalePageMessageIds,
        onError: (messageId, error) => {
          logError('[q_catalog] 删除旧目录分页消息失败', {
            channelId: channelIdRaw,
            staleMessageId: messageId,
            error,
          });
        },
      });

      const detailPageCountByCollection = Object.fromEntries(
        Object.entries(nextCollectionNavState?.detailPageMessageIds ?? {}).map(([name, ids]) => [name, ids.length]),
      ) as Record<string, number>;

      pruneCatalogHashState({
        hashState: nextHashState,
        mainPageCount: hasMainCatalogContent ? pageContents.length : 0,
        indexPageCount: nextCollectionNavState?.indexPageMessageIds?.length ?? 0,
        detailPageCountByCollection,
      });

      try {
        await prisma.channel.update({
          where: { id: channelId },
          data: {
            navMessageId: finalMessageId ? BigInt(finalMessageId) : null,
            lastNavUpdateAt: new Date(),
            navReplyMarkup: mergeCatalogHashStateIntoReplyMarkup(
              mergeCollectionNavStateIntoReplyMarkup(channel.navReplyMarkup, nextCollectionNavState),
              nextHashState,
            ),
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
              navReplyMarkup: mergeCatalogHashStateIntoReplyMarkup(
                mergeCollectionNavStateIntoReplyMarkup(channel.navReplyMarkup, nextCollectionNavState),
                nextHashState,
              ) as any,
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

      const hashGateTotal = hashGateSkipCount + hashGatePublishCount;
      catalogMetrics.hashGateTotal += hashGateTotal;
      catalogMetrics.hashGateSkipTotal += hashGateSkipCount;
      catalogMetrics.hashGatePublishTotal += hashGatePublishCount;

      logger.info('[q_catalog] TypeC自愈修复统计', {
        runId,
        channelId: channelIdRaw,
        triggerType,
        selfHealEnabledOnRun,
        hashGateEnabled: TYPEC_HASH_GATE_ENABLED,
        hashForceRepublish,
        hash_schema_version: hashSchemaVersion,
        hash_gate_total: hashGateTotal,
        hash_gate_skip_count: hashGateSkipCount,
        hash_gate_publish_count: hashGatePublishCount,
        self_heal_fixed_count: selfHealFixedCount,
        self_heal_fallback_count: selfHealFallbackCount,
        self_heal_orphan_cleaned_count: selfHealOrphanCleanedCount,
      });

      const groupRenderedCount = TYPEC_READ_FROM_CATALOG_SOURCE
        ? await (prisma as any).catalogSourceItem.count({ where: { channelId, sourceType: 'group' } })
        : 0;
      const collectionRenderedCount = collectionEpisodesAll.length;

      catalogMetrics.publishItemsRenderedTotal += videos.length;
      catalogMetrics.publishGroupItemsRenderedTotal += Number(groupRenderedCount || 0);
      catalogMetrics.publishCollectionItemsRenderedTotal += Number(collectionRenderedCount || 0);
      catalogMetrics.publishRunSuccessTotal += 1;

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

      const runDurationMs = Date.now() - runStartedAt;
      catalogMetrics.publishDurationMsTotal += runDurationMs;

      logger.info('[q_catalog] 目录发布完成', {
        runId,
        channelId: channelIdRaw,
        triggerType,
        result: 'success',
        dataSource: TYPEC_READ_FROM_CATALOG_SOURCE ? 'catalog_source_item' : 'dispatch_task',
        renderedCount: videos.length,
        groupItemCount: Number(groupRenderedCount || 0),
        collectionItemCount: Number(collectionRenderedCount || 0),
        pageCount: pageContents.length,
        durationMs: runDurationMs,
      });

      return {
        ok: true,
        channelId: channelIdRaw,
        messageId: finalMessageId,
        pageCount: pageContents.length,
        navPageSize,
        runId,
        triggerType,
        durationMs: runDurationMs,
      };
    } catch (error) {
      catalogMetrics.publishRunFailedTotal += 1;
      const runDurationMs = Date.now() - runStartedAt;
      catalogMetrics.publishDurationMsTotal += runDurationMs;

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
        runId,
        channelId: channelIdRaw,
        triggerType,
        result: 'failed',
        dataSource: TYPEC_READ_FROM_CATALOG_SOURCE ? 'catalog_source_item' : 'dispatch_task',
        durationMs: runDurationMs,
        errorName: error instanceof Error ? error.name : 'UnknownError',
        errorMessage: error instanceof Error ? error.message : String(error),
        errorStack: error instanceof Error ? error.stack : null,
        error,
      });
      throw error;
    }
  } finally {
    await releaseChannelLock({
      lockKey: channelLock.lockKey,
      lockToken: channelLock.lockToken,
    });
  }
}

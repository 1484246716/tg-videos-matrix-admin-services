/**
 * Clone Channels 索引服务：从 Telegram 拉取消息并写入 crawl item。
 * 用于在 clone 调度/执行链路中完成增量索引、去重、过滤与下载任务派发。
 */

import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { prisma } from '../../infra/prisma';
import { cloneRetryQueue, cloneMediaDownloadQueue, cloneGroupL1DispatchQueue } from '../../infra/redis';
import { logger } from '../../logger';
import { withClient } from './clone-session.service';
import {
  CloneChannelIndexJob,
  CloneContentType,
  CloneRetryReason,
  IndexedMessageDTO,
} from '../types/clone-queue.types';
import { CLONE_RETRY_MAX } from '../constants/clone-queue.constants';
import { markCloneTaskRunFinished } from './clone-task.service';
import { enqueueCloneGroupItem, shouldUseCloneL1L2 } from './clone-group-scheduler.service';

// 将异常归类为可观测的重试原因码。
function classifyRetryReason(err: unknown): CloneRetryReason {
  const message = err instanceof Error ? err.message.toLowerCase() : '';
  if (message.includes('floodwait') || message.includes('flood_wait')) return 'flood_wait';
  if (message.includes('timeout') || message.includes('network') || message.includes('socket')) return 'network_timeout';
  if (message.includes('auth_invalid') || message.includes('auth')) return 'auth_invalid';
  if (message.includes('channel') && message.includes('unreachable')) return 'channel_unreachable';
  if (message.includes('bot_method_invalid')) return 'auth_invalid';
  return 'index_unknown_error';
}

// 规范化频道用户名：去除 @ 前缀并统一小写。
function normalizeChannelUsername(raw: string) {
  return raw.trim().replace(/^@+/, '').toLowerCase();
}

// 转换为数据库存储格式的频道用户名（补齐 @ 前缀）。
function toDbChannelUsername(normalized: string) {
  return `@${normalized}`;
}

// 解析单条消息链接（t.me/channel/messageId）并返回标准化结果。
function parseSingleMessageLink(raw?: string | null) {
  const value = String(raw ?? '').trim();
  if (!value) return null;

  const matched = value.match(
    /^(?:https?:\/\/)?(?:www\.)?t\.me\/([a-zA-Z0-9_]{5,})\/(\d+)(?:[/?#].*)?$/i,
  );

  if (!matched) {
    throw new Error('invalid single message link, expected format like https://t.me/channel/123');
  }

  const messageId = BigInt(matched[2]);
  if (messageId <= 0n) {
    throw new Error('invalid single message link message id');
  }

  return {
    channelUsername: normalizeChannelUsername(matched[1]),
    messageId,
    normalizedLink: `https://t.me/${matched[1]}/${matched[2]}`,
  };
}

function decodeSingleMessageLinks(raw?: string | null) {
  const value = String(raw ?? '').trim();
  if (!value) return [] as string[];

  if (value.startsWith('{')) {
    try {
      const parsed = JSON.parse(value) as { links?: string[] };
      if (Array.isArray(parsed.links)) {
        return parsed.links.map((item) => String(item || '').trim()).filter(Boolean);
      }
    } catch {
      // fallback as legacy single link
    }
  }

  return [value];
}

function parseSingleMessageLinks(raw?: string | null) {
  const links = decodeSingleMessageLinks(raw);
  const map = new Map<string, NonNullable<ReturnType<typeof parseSingleMessageLink>>>();

  for (const link of links) {
    const parsed = parseSingleMessageLink(link);
    if (!parsed) continue;
    const key = `${parsed.channelUsername}:${parsed.messageId.toString()}`;
    if (!map.has(key)) map.set(key, parsed);
  }

  return Array.from(map.values());
}

// 解析并校验内容类型过滤项，非法值自动忽略并回退默认集合。
function parseContentTypes(raw: string[] | CloneContentType[] | undefined): CloneContentType[] {
  const allowed: CloneContentType[] = ['text', 'image', 'video'];
  if (!raw || raw.length === 0) return allowed;
  const normalized = raw
    .map((t) => String(t).toLowerCase())
    .filter((t): t is CloneContentType => allowed.includes(t as CloneContentType));
  return normalized.length ? Array.from(new Set(normalized)) : allowed;
}

const AD_KEYWORDS = [
  '广告',
  '互推',
  '商务合作',
  '商务联系',
  '合作联系',
  '招商',
  '博彩',
  '娱乐城',
  '下注',
  '注册送彩金',
  '返水',
  '代理',
  '上分',
  '下分',
  '平台客服',
  '推广',
  '点击注册',
  '官网地址',
  '最新网址',
  'tgads',
  'bet',
  'casino',
  'bonus',
  'affiliate',
  'promo code',
  'usdt',
  // 保持关键词为“强广告语义”，避免误伤正常图文媒体
];

const AD_HASHTAGS = ['#广告', '#ad', '#推广', '#博彩'];

const AD_BUTTON_KEYWORDS = [
  '注册',
  '开户',
  '官网',
  '客服',
  '联系',
  '下载',
  '立即体验',
  '点击进入',
  '充值',
  '返利',
  '礼包',
  // 保持按钮关键词为强广告语义，避免误伤正常图文
];

const SUSPICIOUS_LINK_KEYWORDS = [
  'casino',
  'bet',
  'bonus',
  'promo',
  'affiliate',
  '开户链接',
  '注册',
  'tgads',
  '博彩',
];

type AdFilterReason =
  | 'keyword'
  | 'hashtag'
  | 'link'
  | 'button'
  | 'pinned_service'
  | 'image_with_blue_link'
  | 'video_with_blue_link'
  | 'text_with_blue_link_and_button';

// 规范化文本：去首尾空白并统一为小写，便于规则匹配。
function normalizeText(value: unknown) {
  return String(value ?? '').trim().toLowerCase();
}

// 提取消息正文与实体中的所有链接。
function extractMessageLinks(msg: any): string[] {
  const text = String(msg?.message ?? '');
  const textLinks = text.match(/https?:\/\/[^\s]+/gi) ?? [];

  const entities = Array.isArray(msg?.entities) ? msg.entities : [];
  const entityLinks = entities
    .map((e: any) => {
      if (typeof e?.url === 'string' && e.url.trim()) return e.url.trim();
      if (typeof e?.text === 'string' && /^https?:\/\//i.test(e.text.trim())) return e.text.trim();
      return '';
    })
    .filter(Boolean);

  return Array.from(new Set([...textLinks, ...entityLinks].map((v) => String(v).trim())));
}

// 提取按钮文案与按钮链接，用于广告/引流规则判断。
function extractButtonTextsAndLinks(msg: any): { texts: string[]; links: string[] } {
  const rows = Array.isArray(msg?.replyMarkup?.rows) ? msg.replyMarkup.rows : [];
  const texts: string[] = [];
  const links: string[] = [];

  for (const row of rows) {
    const buttons = Array.isArray(row?.buttons) ? row.buttons : [];
    for (const btn of buttons) {
      if (typeof btn?.text === 'string' && btn.text.trim()) texts.push(btn.text.trim());
      if (typeof btn?.url === 'string' && btn.url.trim()) links.push(btn.url.trim());
    }
  }

  return { texts, links };
}

// 根据实体类型判断消息中是否存在“蓝链”。
function hasBlueLinkByEntity(msg: any) {
  const entities = Array.isArray(msg?.entities) ? msg.entities : [];
  return entities.some((e: any) => {
    const className = String(e?.className ?? '').toLowerCase();
    return className.includes('messageentityurl') || className.includes('messageentitytexturl');
  });
}

// 判断消息是否携带回复按钮。
function hasReplyButtons(msg: any) {
  const rows = Array.isArray(msg?.replyMarkup?.rows) ? msg.replyMarkup.rows : [];
  return rows.some((row: any) => Array.isArray(row?.buttons) && row.buttons.length > 0);
}

function createEmptyAdFilterStats(): Record<AdFilterReason, number> {
  return {
    keyword: 0,
    hashtag: 0,
    link: 0,
    button: 0,
    pinned_service: 0,
    image_with_blue_link: 0,
    video_with_blue_link: 0,
    text_with_blue_link_and_button: 0,
  };
}

// 命中广告/引流规则检测并返回原因集合。
function detectAdReasons(msg: any, options?: { bypassFilter?: boolean }): AdFilterReason[] {
  if (options?.bypassFilter) {
    return [];
  }

  const reasons = new Set<AdFilterReason>();
  const messageText = normalizeText(msg?.message);

  const media = (msg as any)?.media;
  const document = (media as any)?.document;
  const photo = (media as any)?.photo;
  const docMimeType = typeof document?.mimeType === 'string' ? document.mimeType.toLowerCase() : '';
  const hasVideoAttr = ((document?.attributes ?? []) as any[]).some((attr) =>
    String(attr?.className ?? '').toLowerCase().includes('video'),
  );
  const hasVideo = Boolean(docMimeType.startsWith('video/') || hasVideoAttr);
  const hasImage = Boolean(docMimeType.startsWith('image/') || photo);
  const hasText = messageText.length > 0;

  // Telegram 服务消息（例如 pinned a message）直接过滤
  const actionClassName = String((msg as any)?.action?.className ?? '').toLowerCase();
  if (actionClassName.includes('messageactionpinmessage')) {
    reasons.add('pinned_service');
  }

  const messageLinks = extractMessageLinks(msg).map((v) => v.toLowerCase());
  const { texts: buttonTexts, links: buttonLinks } = extractButtonTextsAndLinks(msg);
  const allLinks = [...messageLinks, ...buttonLinks.map((v) => v.toLowerCase())];
  const hasBlueLink = hasBlueLinkByEntity(msg) || allLinks.length > 0;
  const hasButtons = hasReplyButtons(msg);

  // 规则1：图片 + 蓝色链接
  if (hasImage && hasBlueLink) {
    reasons.add('image_with_blue_link');
  }

  // 规则3/4：视频 + 蓝色链接（是否有按钮都过滤）
  if (hasVideo && hasBlueLink) {
    reasons.add('video_with_blue_link');
  }

  // 规则2：文字 + 蓝色链接 + 按钮
  if (hasText && !hasImage && !hasVideo && hasBlueLink && hasButtons) {
    reasons.add('text_with_blue_link_and_button');
  }

  if (AD_KEYWORDS.some((kw) => messageText.includes(kw.toLowerCase()))) {
    reasons.add('keyword');
  }

  if (AD_HASHTAGS.some((tag) => messageText.includes(tag.toLowerCase()))) {
    reasons.add('hashtag');
  }

  if (
    allLinks.some((link) =>
      SUSPICIOUS_LINK_KEYWORDS.some((kw) => link.includes(kw.toLowerCase())),
    )
  ) {
    reasons.add('link');
  }

  if (
    buttonTexts
      .map((v) => v.toLowerCase())
      .some((text) => AD_BUTTON_KEYWORDS.some((kw) => text.includes(kw.toLowerCase())))
  ) {
    reasons.add('button');
  }

  return Array.from(reasons);
}

// 清理文件名中的非法字符，确保可落盘。
function sanitizeFileName(name: string) {
  return name.replace(/[<>:"/\\|?*\u0000-\u001F]/g, '_').replace(/\s+/g, ' ').trim();
}

// 根据 MIME 类型解析文件扩展名。
function resolveFileExtensionByMime(mimeType?: string | null) {
  const normalized = (mimeType ?? '').toLowerCase();
  if (normalized === 'video/mp4') return '.mp4';
  if (normalized === 'video/webm') return '.webm';
  if (normalized === 'video/x-matroska' || normalized === 'video/mkv') return '.mkv';
  if (normalized === 'image/jpeg' || normalized === 'image/jpg') return '.jpg';
  if (normalized === 'image/png') return '.png';
  if (normalized === 'image/webp') return '.webp';
  if (normalized === 'image/gif') return '.gif';
  return '.bin';
}

// 生成媒体文件基础名（频道-消息-序号）。
function resolveMediaBaseName(params: {
  channelUsername: string;
  messageId: bigint;
  mimeType?: string | null;
  mediaType: 'image' | 'video';
  mediaIndex: number;
}) {
  const ext = resolveFileExtensionByMime(params.mimeType);
  const channelBase = params.channelUsername.replace(/^@/, '');
  const fallbackStem = sanitizeFileName(`${channelBase}-${params.messageId.toString()}`) || 'media';
  return `${fallbackStem}-${params.mediaType}${Math.max(1, params.mediaIndex)}${ext}`;
}

// 解析分组目录名：有 groupedId 用 grouped-，否则使用 single-。
function resolveGroupDirName(groupedId: string | undefined, messageId: bigint) {
  if (groupedId && groupedId.trim()) return `grouped-${sanitizeFileName(groupedId.trim())}`;
  return `single-${messageId.toString()}`;
}

// 计算分组消息的目标落盘路径。
function resolveGroupedTargetPath(baseTargetPath: string, groupedId: string | undefined, messageId: bigint) {
  return path.join(baseTargetPath, resolveGroupDirName(groupedId, messageId));
}

// 推断条目媒体类型（image/video），用于分组计数统计。
function inferCloneItemMediaKind(item: {
  hasVideo: boolean;
  mimeType?: string | null;
  localPath?: string | null;
}) {
  if (item.hasVideo) return 'video' as const;

  const mime = String(item.mimeType ?? '').toLowerCase();
  if (mime.startsWith('image/')) return 'image' as const;
  if (mime.startsWith('video/')) return 'video' as const;

  const pathLike = String(item.localPath ?? '').toLowerCase();
  if (!pathLike) return null;

  if (/(\.mp4|\.mov|\.mkv|\.webm)$/.test(pathLike)) return 'video' as const;
  if (/(\.jpg|\.jpeg|\.png|\.webp)$/.test(pathLike)) return 'image' as const;

  return null;
}

// 持久化消息文本到目标目录 message.txt。
async function persistMessageText(params: {
  targetPath: string;
  channelUsername: string;
  messageId: bigint;
  mimeType?: string | null;
  messageText?: string;
}) {
  const content = (params.messageText ?? '').trim();
  if (!content) return;

  await mkdir(params.targetPath, { recursive: true });

  const txtPath = path.join(params.targetPath, 'message.txt');
  await writeFile(txtPath, content, 'utf8');
}

// 拉取增量消息并按内容类型/广告规则过滤。
async function fetchIncrementalMessages(params: {
  channelUsername: string;
  lastFetchedMessageId?: bigint | null;
  recentLimit: number;
  contentTypes: CloneContentType[];
  specificMessageId?: bigint | null;
}): Promise<IndexedMessageDTO[]> {
  const {
    channelUsername,
    lastFetchedMessageId,
    recentLimit,
    contentTypes,
    specificMessageId,
  } = params;

  // recentLimit 按“消息组”计数：有 groupedId 的按 groupedId 归组；无 groupedId 视作单条独立组
  const groupLimit = Math.max(1, Math.min(1000, recentLimit || 100));
  const rawFetchLimit = Math.max(groupLimit, Math.min(1000, groupLimit * 8));

  logger.info('[clone][索引/Index] 开始拉取 Telegram 消息 / start fetch telegram messages', {
    channelUsername,
    lastFetchedMessageId: lastFetchedMessageId?.toString() ?? null,
    groupLimit,
    rawFetchLimit,
    contentTypes,
  });

  const messages = await withClient({ timeoutMs: 120_000, accountType: 'user' }, async (client) => {
    const entity = await (client as any).getEntity(channelUsername);
    const list = await (client as any).getMessages(entity, {
      limit: rawFetchLimit,
      minId: lastFetchedMessageId ? Number(lastFetchedMessageId) : undefined,
    });
    return Array.isArray(list) ? list : [];
  });

  const selectedGroupKeys = new Set<string>();
  for (const msg of messages) {
    if (selectedGroupKeys.size >= groupLimit) break;
    const messageIdRaw = (msg as any)?.id;
    if (!Number.isFinite(messageIdRaw)) continue;
    const groupedId = (msg as any)?.groupedId ?? (msg as any)?.grouped_id;
    const groupKey = groupedId != null ? `g:${String(groupedId)}` : `m:${String(messageIdRaw)}`;
    selectedGroupKeys.add(groupKey);
  }

  const picked: IndexedMessageDTO[] = [];

  let skippedByNoId = 0;
  let skippedByType = 0;
  let skippedByAd = 0;
  const skippedByAdReasons = createEmptyAdFilterStats();
  let pickedVideo = 0;
  let pickedImage = 0;
  let pickedText = 0;

  for (const msg of messages) {
    const messageIdRaw = (msg as any)?.id;
    if (!Number.isFinite(messageIdRaw)) {
      skippedByNoId += 1;
      continue;
    }

    const groupedId = (msg as any)?.groupedId ?? (msg as any)?.grouped_id;
    const groupKey = groupedId != null ? `g:${String(groupedId)}` : `m:${String(messageIdRaw)}`;
    if (!selectedGroupKeys.has(groupKey)) {
      skippedByType += 1;
      continue;
    }

    const messageText = ((msg as any)?.message ?? '') as string;
    const media = (msg as any)?.media;
    const document = (media as any)?.document;
    const photo = (media as any)?.photo;

    const docMimeType = typeof document?.mimeType === 'string' ? document.mimeType : undefined;
    const maybeSize = Number(document?.size);
    const fileSize = Number.isFinite(maybeSize) && maybeSize > 0 ? BigInt(Math.floor(maybeSize)) : undefined;

    const hasVideo = Boolean(
      docMimeType?.toLowerCase().startsWith('video/') ||
      ((document?.attributes ?? []) as any[]).some((attr) => String(attr?.className ?? '').toLowerCase().includes('video')),
    );

    const hasImage = Boolean(docMimeType?.toLowerCase().startsWith('image/') || photo);
    const hasText = Boolean(messageText && messageText.trim().length > 0);
    const mimeType = hasVideo ? docMimeType : hasImage ? docMimeType ?? 'image/jpeg' : docMimeType;

    let include = false;
    if (hasVideo && contentTypes.includes('video')) include = true;
    else if (hasImage && contentTypes.includes('image')) include = true;
    else if (hasText && contentTypes.includes('text')) include = true;

    if (!include) {
      skippedByType += 1;
      continue;
    }

    const adReasons = detectAdReasons(msg as any);

    if (adReasons.length > 0) {
      const hasButtons = hasReplyButtons(msg as any);
      const hasBlueLink = hasBlueLinkByEntity(msg as any) || extractMessageLinks(msg as any).length > 0;

      const messageTextPreview = messageText
        ? `${messageText.replace(/\s+/g, ' ').trim().slice(0, 10)}***`
        : '';

      logger.info('[clone][索引/Index] 消息被过滤（广告/引流判定）', {
        channelUsername,
        messageId: String(messageIdRaw),
        reasons: adReasons,
        hasImage,
        hasVideo,
        hasButtons,
        hasBlueLink,
        messageTextPreview,
      });

      skippedByAd += 1;
      for (const reason of adReasons) {
        skippedByAdReasons[reason] += 1;
      }
      continue;
    }

    if (hasVideo) pickedVideo += 1;
    else if (hasImage) pickedImage += 1;
    else if (hasText) pickedText += 1;

    const groupedIdString = groupedId != null ? String(groupedId) : undefined;

    picked.push({
      messageId: BigInt(Math.floor(messageIdRaw)),
      groupedId: groupedIdString,
      groupKey: groupedIdString ? `grouped-${groupedIdString}` : `single-${Math.floor(messageIdRaw)}`,
      messageDate: (msg as any)?.date ? new Date((msg as any).date) : undefined,
      messageText,
      hasVideo,
      fileSize,
      mimeType,
      mediaRef: `tg://message/${channelUsername}/${messageIdRaw}`,
    });
  }

  logger.info('[clone][索引/Index] Telegram 消息过滤完成 / telegram message filtering done', {
    channelUsername,
    fetchedRaw: messages.length,
    groupLimit,
    selectedGroupCount: selectedGroupKeys.size,
    picked: picked.length,
    pickedVideo,
    pickedImage,
    pickedText,
    skippedByNoId,
    skippedByType,
    skippedByAd,
    skippedByAdReasons,
  });

  return picked;
}

// 拉取指定单条消息（若属于 groupedId 则补全同组消息）。
async function fetchSingleMessage(params: {
  channelUsername: string;
  messageId: bigint;
  contentTypes: CloneContentType[];
}): Promise<IndexedMessageDTO[]> {
  const { channelUsername, messageId, contentTypes } = params;

  logger.info('[clone][single] start fetch single telegram message', {
    channelUsername,
    messageId: messageId.toString(),
    contentTypes,
  });

  const { messages, targetGroupedId } = await withClient({ timeoutMs: 120_000, accountType: 'user' }, async (client) => {
    const entity = await (client as any).getEntity(channelUsername);
    const targetList = await (client as any).getMessages(entity, {
      ids: [Number(messageId)],
    });
    const targetMessages = Array.isArray(targetList) ? targetList : targetList ? [targetList] : [];

    const target = targetMessages.find((m: any) => Number.isFinite((m as any)?.id) && BigInt(Math.floor((m as any).id)) === messageId);
    if (!target) {
      return { messages: [] as any[], targetGroupedId: null as string | null };
    }

    const groupedIdRaw = (target as any)?.groupedId ?? (target as any)?.grouped_id;
    const groupedId = groupedIdRaw != null ? String(groupedIdRaw) : null;

    if (!groupedId) {
      return { messages: [target], targetGroupedId: null as string | null };
    }

    const center = Number(messageId);
    const range = 40;
    const ids = Array.from({ length: range * 2 + 1 }, (_v, i) => center - range + i).filter((id) => id > 0);
    const aroundList = await (client as any).getMessages(entity, { ids });
    const aroundMessages = Array.isArray(aroundList) ? aroundList : aroundList ? [aroundList] : [];

    const groupedMessages = aroundMessages.filter((m: any) => {
      const mGroupedId = (m as any)?.groupedId ?? (m as any)?.grouped_id;
      return mGroupedId != null && String(mGroupedId) === groupedId;
    });

    return {
      messages: groupedMessages.length > 0 ? groupedMessages : [target],
      targetGroupedId: groupedId,
    };
  });

  const picked: IndexedMessageDTO[] = [];
  let skippedByNoId = 0;
  let skippedByType = 0;
  let skippedByAd = 0;
  const skippedByAdReasons = createEmptyAdFilterStats();
  let foundTargetMessage = false;

  for (const msg of messages) {
    const messageIdRaw = (msg as any)?.id;
    if (!Number.isFinite(messageIdRaw)) {
      skippedByNoId += 1;
      continue;
    }

    if (BigInt(Math.floor(messageIdRaw)) === messageId) {
      foundTargetMessage = true;
    }

    const messageText = ((msg as any)?.message ?? '') as string;
    const media = (msg as any)?.media;
    const document = (media as any)?.document;
    const photo = (media as any)?.photo;

    const docMimeType = typeof document?.mimeType === 'string' ? document.mimeType : undefined;
    const maybeSize = Number(document?.size);
    const fileSize = Number.isFinite(maybeSize) && maybeSize > 0 ? BigInt(Math.floor(maybeSize)) : undefined;

    const hasVideo = Boolean(
      docMimeType?.toLowerCase().startsWith('video/') ||
      ((document?.attributes ?? []) as any[]).some((attr) => String(attr?.className ?? '').toLowerCase().includes('video')),
    );

    const hasImage = Boolean(docMimeType?.toLowerCase().startsWith('image/') || photo);
    const hasText = Boolean(messageText && messageText.trim().length > 0);
    const mimeType = hasVideo ? docMimeType : hasImage ? docMimeType ?? 'image/jpeg' : docMimeType;

    let include = false;
    if (hasVideo && contentTypes.includes('video')) include = true;
    else if (hasImage && contentTypes.includes('image')) include = true;
    else if (hasText && contentTypes.includes('text')) include = true;

    if (!include) {
      skippedByType += 1;
      continue;
    }

    const adReasons = detectAdReasons(msg as any, { bypassFilter: true });
    if (adReasons.length > 0) {
      skippedByAd += 1;
      for (const reason of adReasons) {
        skippedByAdReasons[reason] += 1;
      }
      continue;
    }

    const groupedId = (msg as any)?.groupedId ?? (msg as any)?.grouped_id;
    const groupedIdString = groupedId != null ? String(groupedId) : undefined;
    const resolvedMessageId = BigInt(Math.floor(messageIdRaw));

    picked.push({
      messageId: resolvedMessageId,
      groupedId: groupedIdString,
      groupKey: groupedIdString ? `grouped-${groupedIdString}` : `single-${resolvedMessageId.toString()}`,
      messageDate: (msg as any)?.date ? new Date((msg as any).date) : undefined,
      messageText,
      hasVideo,
      fileSize,
      mimeType,
      mediaRef: `tg://message/${channelUsername}/${resolvedMessageId.toString()}`,
    });
  }

  if (!foundTargetMessage) {
    throw new Error(`channel_unreachable: message not found tg://${channelUsername}/${messageId.toString()}`);
  }

  logger.info('[clone][single] single message fetch done', {
    channelUsername,
    messageId: messageId.toString(),
    groupedId: targetGroupedId,
    fetchedRaw: messages.length,
    picked: picked.length,
    skippedByNoId,
    skippedByType,
    skippedByAd,
    skippedByAdReasons,
  });

  return picked;
}

// 批量写入索引条目：去重、落库并按策略派发下载任务。
async function upsertIndexedItems(params: {
  taskId: bigint;
  runId: bigint;
  channelUsername: string;
  items: IndexedMessageDTO[];
  crawlMode: 'index_only' | 'index_and_download';
  targetPath: string;
}): Promise<{ inserted: number; deduped: number; queuedDownloads: number; maxMessageId?: bigint }> {
  let inserted = 0;
  let deduped = 0;
  let queuedDownloads = 0;
  let maxMessageId: bigint | undefined;
  const groupMediaIndex = new Map<string, { image: number; video: number }>();

  for (const row of params.items) {
    if (!maxMessageId || row.messageId > maxMessageId) maxMessageId = row.messageId;

    const exists = await prisma.cloneCrawlItem.findUnique({
      where: {
        channelUsername_messageId: {
          channelUsername: params.channelUsername,
          messageId: row.messageId,
        },
      },
      select: { id: true },
    });

    if (exists) {
      deduped += 1;
      continue;
    }

    const isDownloadableMedia = row.hasVideo || (row.mimeType ?? '').toLowerCase().startsWith('image/');
    const groupedTargetPath = resolveGroupedTargetPath(params.targetPath, row.groupedId, row.messageId);

    const item = await prisma.cloneCrawlItem.create({
      data: {
        taskId: params.taskId,
        runId: params.runId,
        channelUsername: params.channelUsername,
        messageId: row.messageId,
        groupedId: row.groupedId,
        groupKey: row.groupKey,
        messageDate: row.messageDate,
        messageText: row.messageText,
        hasVideo: row.hasVideo,
        fileSize: row.fileSize,
        mimeType: row.mimeType,
        localPath: groupedTargetPath,
        downloadStatus:
          params.crawlMode === 'index_and_download' && isDownloadableMedia ? 'queued' : 'none',
      } as any,
    });

    inserted += 1;

    if (row.messageText && row.messageText.trim()) {
      await persistMessageText({
        targetPath: groupedTargetPath,
        channelUsername: params.channelUsername,
        messageId: row.messageId,
        mimeType: row.mimeType,
        messageText: row.messageText,
      });
    }

    if (params.crawlMode === 'index_and_download' && isDownloadableMedia) {
      const groupCounter = groupMediaIndex.get(row.groupKey) ?? { image: 0, video: 0 };
      const mediaType: 'image' | 'video' = row.hasVideo ? 'video' : 'image';
      if (mediaType === 'video') {
        groupCounter.video += 1;
      } else {
        groupCounter.image += 1;
      }
      groupMediaIndex.set(row.groupKey, groupCounter);

      const mediaIndex = mediaType === 'video' ? groupCounter.video : groupCounter.image;

      const downloadJob = {
        taskId: params.taskId.toString(),
        runId: params.runId.toString(),
        itemId: item.id.toString(),
        channelUsername: params.channelUsername,
        mediaRef: row.mediaRef
          ? {
              kind: 'tg_message' as const,
              channelUsername: params.channelUsername,
              messageId: row.messageId.toString(),
            }
          : undefined,
        expectedFileSize: row.fileSize ? row.fileSize.toString() : undefined,
        expectedMimeType: row.mimeType,
        groupedId: row.groupedId,
        groupKey: row.groupKey,
        expectedFileName: resolveMediaBaseName({
          channelUsername: params.channelUsername,
          messageId: row.messageId,
          mimeType: row.mimeType,
          mediaType,
          mediaIndex,
        }),
        targetPath: groupedTargetPath,
        priority:
          row.fileSize && row.fileSize > BigInt(1024 * 1024 * 1024)
            ? 'large'
            : row.fileSize && row.fileSize < BigInt(200 * 1024 * 1024)
              ? 'small'
              : 'medium',
        enqueuedAt: new Date().toISOString(),
      };

      if (shouldUseCloneL1L2(downloadJob)) {
        logger.info('[clone][l1l2] route grouped job to l1l2', {
          taskId: params.taskId.toString(),
          runId: params.runId.toString(),
          itemId: item.id.toString(),
          groupKey: row.groupKey,
          groupedId: row.groupedId,
        });
        await enqueueCloneGroupItem(downloadJob);
        await cloneGroupL1DispatchQueue.add(
          'clone-group-l1-dispatch-tick',
          {
            source: 'clone-index',
            runId: params.runId.toString(),
            taskId: params.taskId.toString(),
            groupKey: row.groupKey,
            at: new Date().toISOString(),
          },
          { removeOnComplete: true, removeOnFail: 100 },
        );
      } else {
        logger.info('[clone][l1l2] route job to legacy download queue', {
          taskId: params.taskId.toString(),
          runId: params.runId.toString(),
          itemId: item.id.toString(),
          groupKey: row.groupKey,
          groupedId: row.groupedId,
          reason: 'l1l2_disabled_or_not_grouped',
        });
        await cloneMediaDownloadQueue.add(
          'clone-media-download',
          downloadJob,
          {
            jobId: `clone-download-item-${item.id.toString()}`,
            removeOnComplete: true,
            removeOnFail: 100,
          },
        );
      }

      queuedDownloads += 1;
      logger.info('[clone][索引/Index] 媒体下载任务已入队 / media download job enqueued', {
        taskId: params.taskId.toString(),
        runId: params.runId.toString(),
        channelUsername: params.channelUsername,
        itemId: item.id.toString(),
        mimeType: row.mimeType ?? null,
        queue: shouldUseCloneL1L2(downloadJob)
          ? cloneGroupL1DispatchQueue.name
          : cloneMediaDownloadQueue.name,
      });
    }
  }

  logger.info('[clone][索引/Index] 明细入库完成 / indexed items persisted', {
    taskId: params.taskId.toString(),
    runId: params.runId.toString(),
    channelUsername: params.channelUsername,
    inputCount: params.items.length,
    inserted,
    deduped,
    queuedDownloads,
    maxMessageId: maxMessageId?.toString() ?? null,
  });

  return { inserted, deduped, queuedDownloads, maxMessageId };
}

// 将爬虫统计的分组媒体数量写入 sourceExpectedCount。
async function writeGroupSourceExpectedCountFromCrawl(params: {
  taskId: bigint;
  channelId: bigint;
  channelUsername: string;
}) {
  const normalizedChannelUsername = normalizeChannelUsername(params.channelUsername);
  const dbChannelUsername = toDbChannelUsername(normalizedChannelUsername);

  const groupedItems = await prisma.cloneCrawlItem.findMany({
    where: {
      taskId: params.taskId,
      channelUsername: {
        in: [normalizedChannelUsername, dbChannelUsername],
      },
      groupKey: { not: null },
    },
    select: {
      groupKey: true,
      groupedId: true,
      hasVideo: true,
      mimeType: true,
      localPath: true,
    },
  });

  if (groupedItems.length === 0) {
    logger.warn('【爬虫分组计数】本次索引未找到可统计的分组媒体', {
      任务ID: params.taskId.toString(),
      频道ID: params.channelId.toString(),
      频道用户名: params.channelUsername,
      标准化频道用户名: normalizedChannelUsername,
      数据库频道用户名候选: [normalizedChannelUsername, dbChannelUsername],
    });
    return;
  }

  const groupedCounter = new Map<string, { total: number; video: number; image: number; groupedId: string | null }>();

  for (const item of groupedItems) {
    const groupKey = item.groupKey?.trim();
    if (!groupKey) continue;

    const mediaKind = inferCloneItemMediaKind(item);
    if (!mediaKind) continue;

    const bucket = groupedCounter.get(groupKey) ?? {
      total: 0,
      video: 0,
      image: 0,
      groupedId: item.groupedId ?? null,
    };

    bucket.total += 1;
    if (mediaKind === 'video') bucket.video += 1;
    else bucket.image += 1;

    if (!bucket.groupedId && item.groupedId) bucket.groupedId = item.groupedId;
    groupedCounter.set(groupKey, bucket);
  }

  for (const [groupKey, stat] of groupedCounter) {
    const latestGroupTask = await (prisma as any).dispatchGroupTask.findFirst({
      where: {
        channelId: params.channelId,
        groupKey,
      },
      orderBy: [{ updatedAt: 'desc' }, { id: 'desc' }],
      select: {
        id: true,
        sourceExpectedCount: true,
      },
    });

    if (!latestGroupTask?.id) {
      const now = new Date();
      const created = await (prisma as any).dispatchGroupTask.create({
        data: {
          channelId: params.channelId,
          groupKey,
          scheduleSlot: now,
          status: 'pending',
          retryCount: 0,
          maxRetries: 6,
          nextRunAt: now,
          expectedMediaCount: stat.total,
          sourceExpectedCount: stat.total,
          actualReadyCount: 0,
          actualUploadedCount: 0,
          lastArrivalAt: now,
        },
        select: {
          id: true,
          sourceExpectedCount: true,
        },
      });

      logger.info('【爬虫分组计数】统计完成并写入 sourceExpectedCount', {
        频道ID: params.channelId.toString(),
        组键: groupKey,
        分组ID: stat.groupedId,
        统计视频数: stat.video,
        统计图片数: stat.image,
        统计媒体总数: stat.total,
        原值: 0,
        新值: Number(created.sourceExpectedCount ?? 0),
        写入策略: 'GREATEST',
        是否更新: true,
        写入方式: 'create_placeholder_dispatch_group_task',
        dispatchGroupTaskId: created.id.toString(),
      });
      continue;
    }

    const oldValue = Number(latestGroupTask.sourceExpectedCount ?? 0);
    const newValue = Math.max(oldValue, stat.total);

    const updated = await (prisma as any).dispatchGroupTask.update({
      where: { id: latestGroupTask.id },
      data: {
        expectedMediaCount: Math.max(stat.total, 0),
        sourceExpectedCount: newValue,
      },
      select: {
        id: true,
        sourceExpectedCount: true,
      },
    });

    logger.info('【爬虫分组计数】统计完成并写入 sourceExpectedCount', {
      频道ID: params.channelId.toString(),
      组键: groupKey,
      分组ID: stat.groupedId,
      统计视频数: stat.video,
      统计图片数: stat.image,
      统计媒体总数: stat.total,
      原值: oldValue,
      新值: Number(updated.sourceExpectedCount ?? 0),
      写入策略: 'GREATEST',
      是否更新: Number(updated.sourceExpectedCount ?? 0) !== oldValue,
      写入方式: 'update_existing_dispatch_group_task',
      dispatchGroupTaskId: updated.id.toString(),
    });
  }
}

// clone 索引主流程：拉取消息、写入 crawl item，并处理重试与收尾。
export async function processCloneChannelIndex(job: CloneChannelIndexJob) {
  const runId = BigInt(job.runId);
  const taskId = BigInt(job.taskId);
  const channelUsername = normalizeChannelUsername(job.channelUsername);
  const dbChannelUsername = toDbChannelUsername(channelUsername);

  logger.info('[clone][索引/Index] 开始处理频道索引 / start channel indexing', {
    taskId: job.taskId,
    runId: job.runId,
    channelUsername,
    retryCount: job.retryCount ?? 0,
  });

  try {
    const task = await prisma.cloneCrawlTask.findUnique({
      where: { id: taskId },
      select: {
        id: true,
        crawlMode: true,
        targetPath: true,
        recentLimit: true,
        singleMessageEnabled: true,
        singleMessageLink: true,
        contentTypes: true,
        scheduleType: true,
        timezone: true,
      },
    });

    if (!task) {
      logger.warn('[clone][索引/Index] 任务不存在，跳过 / task not found, skip indexing', {
        taskId: job.taskId,
        runId: job.runId,
        channelUsername,
      });
      return;
    }

    const channel = await prisma.cloneCrawlTaskChannel.findUnique({
      where: {
        taskId_channelUsername: {
          taskId,
          channelUsername: dbChannelUsername,
        },
      },
      select: {
        id: true,
        channelUsername: true,
        channelAccessStatus: true,
        lastFetchedMessageId: true,
      },
    });

    if (!channel) {
      throw new Error(`channel_unreachable: task channel not found for ${dbChannelUsername}`);
    }

    if (channel.channelAccessStatus !== 'ok') {
      throw new Error(`channel_unreachable: channel access status is ${channel.channelAccessStatus}`);
    }

    const lastFetchedMessageId = job.lastFetchedMessageId
      ? BigInt(job.lastFetchedMessageId)
      : channel?.lastFetchedMessageId ?? null;
    const singleMessageTargets = task.singleMessageEnabled
      ? parseSingleMessageLinks(task.singleMessageLink)
      : [];

    if (task.singleMessageEnabled && singleMessageTargets.length === 0) {
      throw new Error('single message mode enabled but no valid message links provided');
    }

    if (singleMessageTargets.some((item) => item.channelUsername !== channelUsername)) {
      throw new Error(
        `invalid single message link channel mismatch: task=${channelUsername}`,
      );
    }

    const contentTypes = parseContentTypes(job.contentTypes ?? (task.contentTypes as string[]));
    const messages = singleMessageTargets.length > 0
      ? Array.from(
          new Map(
            (
              await Promise.all(
                singleMessageTargets.map((target) =>
                  fetchSingleMessage({
                    channelUsername,
                    messageId: target.messageId,
                    contentTypes,
                  }),
                ),
              )
            )
              .flat()
              .map((item) => [`${item.messageId.toString()}:${item.groupKey}`, item] as const),
          ).values(),
        )
      : await fetchIncrementalMessages({
          channelUsername,
          lastFetchedMessageId,
          recentLimit: job.recentLimit ?? task.recentLimit,
          contentTypes,
        });

    const { inserted, deduped, queuedDownloads, maxMessageId } = await upsertIndexedItems({
      taskId,
      runId,
      channelUsername,
      items: messages,
      crawlMode: task.crawlMode,
      targetPath: task.targetPath,
    });

    if (maxMessageId && channel) {
      await prisma.cloneCrawlTaskChannel.update({
        where: { id: channel.id },
        data: {
          lastFetchedMessageId: maxMessageId,
          lastRunAt: new Date(),
          lastErrorCode: null,
          lastErrorMessage: null,
        },
      });
    }

    const mappedChannel = await prisma.channel.findFirst({
      where: {
        folderPath: task.targetPath,
      },
      select: { id: true, folderPath: true, tgUsername: true },
    });

    if (mappedChannel?.id) {
      await writeGroupSourceExpectedCountFromCrawl({
        taskId,
        channelId: mappedChannel.id,
        channelUsername,
      });
    } else {
      logger.warn('【爬虫分组计数】未匹配到业务频道，跳过 sourceExpectedCount 写入', {
        taskId: job.taskId,
        runId: job.runId,
        channelUsername,
        taskTargetPath: task.targetPath,
      });
    }

    await prisma.$transaction(async (tx) => {
      await tx.cloneCrawlRun.update({
        where: { id: runId },
        data: {
          status: 'running',
          indexedCount: { increment: inserted },
          dedupCount: { increment: deduped },
          channelSuccess: { increment: 1 },
          downloadQueued:
            task.crawlMode === 'index_and_download'
              ? { increment: queuedDownloads }
              : undefined,
        },
      });

      const runAfterIncrement = await tx.cloneCrawlRun.findUnique({
        where: { id: runId },
        select: { id: true, channelTotal: true, channelSuccess: true, channelFailed: true },
      });

      const shouldFinish =
        (runAfterIncrement?.channelTotal ?? 0) > 0 &&
        (runAfterIncrement?.channelSuccess ?? 0) + (runAfterIncrement?.channelFailed ?? 0) >=
          (runAfterIncrement?.channelTotal ?? 0);

      if (shouldFinish) {
        await tx.cloneCrawlRun.update({
          where: { id: runId },
          data: {
            status: 'success',
            finishedAt: new Date(),
          },
        });

        await markCloneTaskRunFinished({
          taskId,
          scheduleType: task.scheduleType,
          timezone: task.timezone,
          tx,
        });
      }
    });

    logger.info('[clone][索引/Index] 频道索引完成 / channel indexing done', {
      taskId: job.taskId,
      runId: job.runId,
      channelUsername,
      crawlMode: task.crawlMode,
      fetched: messages.length,
      inserted,
      deduped,
      maxMessageId: maxMessageId?.toString() ?? null,
    });
  } catch (err) {
    const reason = classifyRetryReason(err);
    const errorMessage = err instanceof Error ? err.message : String(err);
    const currentRetryCount = job.retryCount ?? 0;
    const nextRetryCount = currentRetryCount + 1;

    const retryAfterSec = (() => {
      const m = errorMessage.match(/flood[_\s]?wait[_\s]?(\d+)/i);
      if (!m) return undefined;
      const n = Number(m[1]);
      return Number.isFinite(n) && n > 0 ? Math.floor(n) : undefined;
    })();

    const nonRetryable = reason === 'auth_invalid' || reason === 'channel_unreachable';
    const shouldRetry = !nonRetryable && nextRetryCount <= CLONE_RETRY_MAX;

    if (shouldRetry) {
      await cloneRetryQueue.add(
        'clone-index-retry',
        {
          queue: 'index',
          reason,
          retryCount: currentRetryCount,
          retryAfterSec,
          payload: {
            ...job,
            channelUsername,
            retryCount: nextRetryCount,
          },
          firstFailedAt: new Date().toISOString(),
          lastErrorMessage: errorMessage,
        },
        { removeOnComplete: true, removeOnFail: 100 },
      );
    }

    await prisma.cloneCrawlTaskChannel.updateMany({
      where: {
        taskId,
        channelUsername: dbChannelUsername,
      },
      data: {
        lastRunAt: new Date(),
        lastErrorCode: reason,
        lastErrorMessage: errorMessage,
      },
    });

    if (!shouldRetry) {
      await prisma.$transaction(async (tx) => {
        await tx.cloneCrawlRun.updateMany({
          where: { id: runId },
          data: {
            status: 'running',
            channelFailed: { increment: 1 },
          },
        });

        const runAfterIncrement = await tx.cloneCrawlRun.findUnique({
          where: { id: runId },
          select: {
            id: true,
            channelTotal: true,
            channelSuccess: true,
            channelFailed: true,
            task: {
              select: {
                id: true,
                scheduleType: true,
                timezone: true,
              },
            },
          },
        });

        const shouldFinish =
          (runAfterIncrement?.channelTotal ?? 0) > 0 &&
          (runAfterIncrement?.channelSuccess ?? 0) + (runAfterIncrement?.channelFailed ?? 0) >=
            (runAfterIncrement?.channelTotal ?? 0);

        if (shouldFinish && runAfterIncrement?.task) {
          await tx.cloneCrawlRun.updateMany({
            where: { id: runId },
            data: {
              status: 'failed',
              finishedAt: new Date(),
            },
          });

          await markCloneTaskRunFinished({
            taskId: runAfterIncrement.task.id,
            scheduleType: runAfterIncrement.task.scheduleType,
            timezone: runAfterIncrement.task.timezone,
            tx,
          });
        }
      });
    }

    logger.warn('[clone][索引/Index] 索引失败 / indexing failed', {
      taskId: job.taskId,
      runId: job.runId,
      channelUsername,
      reason,
      shouldRetry,
      currentRetryCount,
      nextRetryCount,
      retryAfterSec,
      queue: shouldRetry ? cloneRetryQueue.name : null,
      error: errorMessage,
    });
  }
}

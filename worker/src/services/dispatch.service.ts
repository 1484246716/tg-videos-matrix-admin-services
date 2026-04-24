/**
 * ?????TypeB ?????????????????????????????????????
 * ?????dispatch.worker -> handleDispatchJob / handleDispatchGroupJob -> Telegram ????????????????????
 */

import { rm } from 'node:fs/promises';
import path from 'node:path';
import { AiModelProfile, DispatchMediaType, MediaStatus, TaskStatus } from '@prisma/client';
import { generateTextWithAiProfile } from '../ai-provider';
import {
  DISPATCH_CHANNEL_INTERVAL_GUARD_ENABLED,
  TYPEB_GROUP_RETRY_CHECK_MS,
  TYPEB_GROUP_SEAL_QUIET_PERIOD_MS,
} from '../config/env';
import { prisma, withPrismaRetry } from '../infra/prisma';
import { searchIndexQueue } from '../infra/redis';
import { logger, logError } from '../logger';
import { getBackoffSeconds } from '../shared/dispatch-utils';
import { catalogSourceWriteMetrics } from '../shared/metrics';
import { sendPhotoByTelegram, sendTelegramRequest, sendVideoByTelegram, TelegramError } from '../shared/telegram';
import { classifyAndAssignForTypeB } from './typeb-category.service';
import { assignContentTagsForTypeB } from './typeb-content-tag.service';

// ?? Parse Entities Error ??????????????????????
function isParseEntitiesError(error: { code?: string; message?: string } | null | undefined) {
  const message = (error?.message ?? '').toLowerCase();
  const code = error?.code ?? '';

  return (
    code === 'TG_400' &&
    (message.includes("can't parse entities") ||
      message.includes('unsupported start tag') ||
      message.includes('unsupported end tag') ||
      message.includes('entity not found') ||
      (message.includes('tag') && message.includes('not closed')))
  );
}

// ?? Deterministic Dispatch Error ??????????????????????
function isDeterministicDispatchError(error: { code?: string; message?: string } | null | undefined) {
  const message = (error?.message ?? '').toLowerCase();

  return (
    isParseEntitiesError(error) ||
    message.includes('媒体资源缺少 telegramfileid') ||
    message.includes('分发任务或频道未配置机器人') ||
    message.includes('未找到可用机器人')
  );
}

// ?? Photo As Video Error ??????????????????????
function isPhotoAsVideoError(error: { code?: string; message?: string } | null | undefined) {
  const message = (error?.message ?? '').toLowerCase();
  return error?.code === 'TG_400' && message.includes("can't use file of type photo as video");
}

// ?? Video As Photo Error ??????????????????????
function isVideoAsPhotoError(error: { code?: string; message?: string } | null | undefined) {
  const message = (error?.message ?? '').toLowerCase();
  return error?.code === 'TG_400' && message.includes("can't use file of type video as photo");
}

// ????? resolve Dispatch Method ????????????????????
function resolveDispatchMethod(meta: Record<string, unknown> | null | undefined, originalName: string) {
  const mediaTypeRaw = typeof meta?.relayResolvedMediaType === 'string' ? meta.relayResolvedMediaType.toLowerCase() : '';
  if (mediaTypeRaw === 'photo') return 'sendPhoto' as const;
  if (mediaTypeRaw === 'video') return 'sendVideo' as const;

  const mimeType = typeof meta?.mimeType === 'string' ? meta.mimeType.toLowerCase() : '';
  if (mimeType.startsWith('image/')) return 'sendPhoto' as const;
  if (mimeType.startsWith('video/')) return 'sendVideo' as const;

  const lowerName = originalName.toLowerCase();
  if (/\.(jpg|jpeg|png|webp)$/i.test(lowerName)) return 'sendPhoto' as const;
  return 'sendVideo' as const;
}

// ?? get File Stem ?????????????????????
function getFileStem(fileName: string) {
  const trimmed = fileName.trim();
  const stem = trimmed.replace(/\.[^./\\]+$/, '').trim();
  return stem || trimmed;
}

// ?? AI Failure Text ??????????????????????
function isAiFailureText(text?: string | null) {
  if (!text) return false;
  const normalized = text.trim();
  if (!normalized) return false;

  return /无法识别|抱歉|如果可以提供更多|视频的内容简介|主要角色|生成相关文案/.test(normalized);
}

// ????? resolve Dispatch AI Profile ????????????????????
async function resolveDispatchAiProfile(aiModelProfileId: bigint | null | undefined): Promise<AiModelProfile | null> {
  const profile = aiModelProfileId
    ? await prisma.aiModelProfile.findUnique({
        where: { id: aiModelProfileId },
      })
    : null;

  if (profile?.isActive) {
    return profile;
  }

  if (!process.env.OPENAI_API_KEY) {
    return null;
  }

  return {
    id: BigInt(0),
    name: 'ENV_FALLBACK',
    provider: 'openai',
    model: process.env.OPENAI_MODEL || 'gpt-3.5-turbo',
    apiKeyEncrypted: process.env.OPENAI_API_KEY,
    endpointUrl: process.env.OPENAI_BASE_URL || null,
    systemPrompt: null,
    captionPromptTemplate: null,
    temperature: null,
    topP: null,
    maxTokens: null,
    timeoutMs: 20000,
    isActive: true,
    fallbackProfileId: null,
    createdAt: new Date(),
    updatedAt: new Date(),
  };
}

// ?? get Collection Display Name ?????????????????????
function getCollectionDisplayName(name: string) {
  const normalized = name.replace(/合集/g, '').trim();
  return normalized || name.trim();
}

// ?? build Collection Episode Title ?????????????????????
function buildCollectionEpisodeTitle(collectionName: string, episodeNo: number) {
  return `${getCollectionDisplayName(collectionName)}第${episodeNo}集`;
}

// ?? apply Collection Episode Title ?????????????????????
function applyCollectionEpisodeTitle(caption: string, title: string) {
  const desiredLine = `📺片名：${title}`;
  const trimmedCaption = caption.trim();
  if (!trimmedCaption) return desiredLine;

  if (/(?:^|\n)\s*📺?\s*片名\s*[：:]\s*.+/.test(trimmedCaption)) {
    return trimmedCaption.replace(/(^|\n)\s*📺?\s*片名\s*[：:]\s*.+/, (_match, prefix: string) => `${prefix}${desiredLine}`);
  }

  return `${desiredLine}\n${trimmedCaption}`;
}

// ??? normalize Title Fallback ????????????????????????
function normalizeTitleFallback(raw: string, fallback: string) {
  const candidate = raw
    .replace(/[《》#]/g, '')
    .replace(/未知/g, '')
    .trim();
  if (candidate) return candidate;

  const cleanFallback = fallback.replace(/[《》#]/g, '').trim();
  return cleanFallback || '精彩视频';
}

// ?? sanitize Type BCaption Unknown ??????????????????????
function sanitizeTypeBCaptionUnknown(caption: string, fallbackTitle: string) {
  let next = caption.trim();
  if (!next) return fallbackTitle;

  const matchedTitle = next.match(/(?:^|\n)\s*📺?\s*片名\s*[：:]\s*(.+)/);
  const titleFromCaption = matchedTitle?.[1]?.trim() || '';
  const safeTitle = normalizeTitleFallback(titleFromCaption, fallbackTitle);

  if (/(?:^|\n)\s*📺?\s*片名\s*[：:]\s*未知\s*(?:\n|$)/.test(next) || /(《\s*#?未知\s*》)/.test(next)) {
    next = applyCollectionEpisodeTitle(next, safeTitle);
  }

  const lines = next.split('\n');
  const firstNonEmptyLineIndex = lines.findIndex((line) => line.trim().length > 0);
  if (firstNonEmptyLineIndex >= 0) {
    const firstLine = lines[firstNonEmptyLineIndex].trim();
    if (firstLine.startsWith('#') && /#\s*未知|《\s*#?未知\s*》/.test(firstLine)) {
      lines[firstNonEmptyLineIndex] = `#视频 #精选 《#${safeTitle}》`;
      next = lines.join('\n');
    }
  }

  return next;
}

// ??? normalize Caption Text ????????????????????????
function normalizeCaptionText(raw?: string | null) {
  if (!raw) return '';
  return raw.replace(/^\uFEFF/, '').trim();
}

// ????? truncate Catalog Title ???????????????
function truncateCatalogTitle(text: string, maxChars = 15) {
  const chars = Array.from(text);
  if (chars.length <= maxChars) return text;
  return `${chars.slice(0, maxChars).join('')}...`;
}

// ???????? extract Catalog Short Title ??????????????????
function extractCatalogShortTitle(raw?: string | null) {
  const caption = normalizeCaptionText(raw);
  if (!caption) return null;

  const matchedTitle = caption.match(/(?:^|\n)\s*📺?\s*片名\s*[：:]\s*(.+)/);
  const candidateSource =
    matchedTitle?.[1]?.trim() ||
    caption
      .split('\n')
      .map((line) => line.trim())
      .find(Boolean) ||
    '';

  const normalized = normalizeTitleFallback(
    candidateSource
      .replace(/^#+\s*/, '')
      .replace(/^[-*]+\s*/, '')
      .replace(/^📺\s*/, '')
      .replace(/^片名\s*[：:]\s*/, '')
      .trim(),
    '精彩视频',
  );

  return truncateCatalogTitle(normalized, 15);
}

// ????? resolve Caption From Source Meta ????????????????????
function resolveCaptionFromSourceMeta(meta: Record<string, unknown> | null | undefined) {
  if (!meta) return '';
  const direct = typeof meta.caption === 'string' ? meta.caption : '';
  if (direct.trim()) return normalizeCaptionText(direct);
  const msg = typeof meta.messageText === 'string' ? meta.messageText : '';
  return normalizeCaptionText(msg);
}

// ?? to Finite Number ?????????????????????
function toFiniteNumber(v: unknown) {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string' && v.trim()) {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

// ?? read Media Dimensions ?????????????????????
function readMediaDimensions(meta: Record<string, unknown> | null | undefined) {
  const width = toFiniteNumber(meta?.width) ?? toFiniteNumber(meta?.videoWidth) ?? toFiniteNumber(meta?.imageWidth);
  const height = toFiniteNumber(meta?.height) ?? toFiniteNumber(meta?.videoHeight) ?? toFiniteNumber(meta?.imageHeight);
  if (!width || !height || width <= 0 || height <= 0) {
    return { width: null, height: null, aspectRatio: null, area: null };
  }
  const aspectRatio = width / height;
  return { width, height, aspectRatio, area: width * height };
}

// ? classify Image Orientation ?????????????????????????
function classifyImageOrientation(aspectRatio: number | null) {
  if (!aspectRatio) return 'square' as const;
  if (aspectRatio >= 1.2) return 'landscape' as const;
  if (aspectRatio <= 0.85) return 'portrait' as const;
  return 'square' as const;
}

// ????? resolve Media Type By Meta ????????????????????
function resolveMediaTypeByMeta(meta: Record<string, unknown> | null | undefined, originalNameHint?: string | null) {
  const relayResolvedMediaType =
    typeof meta?.relayResolvedMediaType === 'string' ? meta.relayResolvedMediaType.toLowerCase() : '';
  if (relayResolvedMediaType === 'photo' || relayResolvedMediaType === 'image') return 'photo' as const;
  if (relayResolvedMediaType === 'video') return 'video' as const;

  const mime = typeof meta?.mimeType === 'string' ? meta.mimeType.toLowerCase() : '';
  if (mime.startsWith('image/')) return 'photo' as const;
  if (mime.startsWith('video/')) return 'video' as const;

  const originalNameFromMeta = typeof meta?.originalName === 'string' ? meta.originalName : '';
  const originalName = (originalNameFromMeta || originalNameHint || '').toLowerCase();
  if (/\.(jpg|jpeg|png|webp|gif|bmp|heic|heif)$/i.test(originalName)) return 'photo' as const;
  if (/\.(mp4|mov|mkv|avi|webm|m4v)$/i.test(originalName)) return 'video' as const;

  const localPath = typeof meta?.localPath === 'string' ? meta.localPath.toLowerCase() : '';
  if (/\.(jpg|jpeg|png|webp|gif|bmp|heic|heif)$/i.test(localPath)) return 'photo' as const;
  if (/\.(mp4|mov|mkv|avi|webm|m4v)$/i.test(localPath)) return 'video' as const;

  return null;
}

// ?? Dispatch Scoped Temp Dir Name ??????????????????????
function isDispatchScopedTempDirName(dirName: string) {
  return /^(single|grouped)-[a-z0-9][a-z0-9_-]*$/i.test(dirName);
}

// ????? resolve Dispatch Scoped Dir From Meta ????????????????????
function resolveDispatchScopedDirFromMeta(
  meta: Record<string, unknown> | null | undefined,
  localPathFallback?: string | null,
) {
  const localPathFromMeta = typeof meta?.localPath === 'string' ? meta.localPath : '';
  const localPath = localPathFromMeta || (typeof localPathFallback === 'string' ? localPathFallback : '');
  if (!localPath) return null;

  const normalized = localPath.replace(/\\/g, '/');
  const match = normalized.match(/(.+\/((?:single|grouped)-[^/]+))\//i);
  if (!match || !match[1] || !match[2]) return null;

  if (!isDispatchScopedTempDirName(match[2])) {
    return null;
  }

  return match[1].replace(/\//g, path.sep);
}

// ?? cleanup Dispatch Scoped Directories After Success ?????????????????????
async function cleanupDispatchScopedDirectoriesAfterSuccess(
  tasks: Array<{ mediaAsset: { sourceMeta: unknown; localPath?: string | null } }>,
  allowedDirNames?: Set<string>,
) {
  const dirs = new Set<string>();
  for (const t of tasks) {
    const meta = t.mediaAsset.sourceMeta && typeof t.mediaAsset.sourceMeta === 'object'
      ? (t.mediaAsset.sourceMeta as Record<string, unknown>)
      : null;
    const dir = resolveDispatchScopedDirFromMeta(meta, t.mediaAsset.localPath ?? null);
    if (dir) dirs.add(dir);
  }

  if (dirs.size === 0) {
    logger.warn('[typeb_cleanup] 跳过目录清理（未解析到 dispatch scoped 目录）', {
      taskCount: tasks.length,
      allowedDirNames: allowedDirNames ? Array.from(allowedDirNames) : null,
    });
    return;
  }

  for (const dir of dirs) {
    const dirName = path.basename(dir);
    if (!isDispatchScopedTempDirName(dirName)) {
      logger.warn('[typeb_cleanup] 跳过目录清理（目录名不安全）', {
        scopedDir: dir,
        scopedDirName: dirName,
      });
      continue;
    }

    if (allowedDirNames && !allowedDirNames.has(dirName)) {
      logger.warn('[typeb_cleanup] 跳过目录清理（不在本次允许列表）', {
        scopedDir: dir,
        scopedDirName: dirName,
        allowedDirNames: Array.from(allowedDirNames),
      });
      continue;
    }

    try {
      await rm(dir, { recursive: true, force: true });
      logger.info('[typeb_cleanup] 发送成功后目录清理完成', {
        scopedDir: dir,
        scopedDirName: dirName,
      });
    } catch (error) {
      logger.warn('[typeb_cleanup] 目录清理失败（忽略）', {
        scopedDir: dir,
        scopedDirName: dirName,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
}

// ?? build Group Caption From Tasks ?????????????????????
function buildGroupCaptionFromTasks(tasks: Array<{ mediaAsset: { sourceMeta: unknown } }>) {
  let firstTxt = '';
  for (const t of tasks) {
    const meta = t.mediaAsset.sourceMeta && typeof t.mediaAsset.sourceMeta === 'object'
      ? (t.mediaAsset.sourceMeta as Record<string, unknown>)
      : null;

    const explicitMessageTxt = typeof meta?.messageTxt === 'string' ? meta.messageTxt : '';
    if (explicitMessageTxt.trim()) {
      return { caption: normalizeCaptionText(explicitMessageTxt), captionSource: 'message_txt' as const };
    }

    const txt = typeof meta?.txtContent === 'string' ? meta.txtContent : '';
    if (!firstTxt && txt.trim()) {
      firstTxt = normalizeCaptionText(txt);
    }
  }

  if (firstTxt) {
    return { caption: firstTxt, captionSource: 'first_txt' as const };
  }

  let longestMessageText = '';
  for (const t of tasks) {
    const meta = t.mediaAsset.sourceMeta && typeof t.mediaAsset.sourceMeta === 'object'
      ? (t.mediaAsset.sourceMeta as Record<string, unknown>)
      : null;
    const text = typeof meta?.messageText === 'string' ? normalizeCaptionText(meta.messageText) : '';
    if (text.length > longestMessageText.length) longestMessageText = text;
  }

  if (longestMessageText) {
    return { caption: longestMessageText, captionSource: 'message_text' as const };
  }

  for (const t of tasks) {
    const meta = t.mediaAsset.sourceMeta && typeof t.mediaAsset.sourceMeta === 'object'
      ? (t.mediaAsset.sourceMeta as Record<string, unknown>)
      : null;
    const fromMeta = resolveCaptionFromSourceMeta(meta);
    if (fromMeta) return { caption: fromMeta, captionSource: 'source_meta' as const };
  }

  return { caption: '', captionSource: 'none' as const };
}

// ?? get Source Meta Object ?????????????????????
function getSourceMetaObject(sourceMeta: unknown) {
  return sourceMeta && typeof sourceMeta === 'object' ? (sourceMeta as Record<string, unknown>) : null;
}

// ?? parse Source Expected Count From Meta ????????????????????????
function parseSourceExpectedCountFromMeta(sourceMeta: unknown) {
  const meta = getSourceMetaObject(sourceMeta);
  const parsed = toFiniteNumber(meta?.sourceExpectedCount);
  if (!parsed || parsed <= 0) return null;
  return Math.floor(parsed);
}

// ?? parse Source Message ID From Meta ????????????????????????
function parseSourceMessageIdFromMeta(sourceMeta: unknown) {
  const meta = getSourceMetaObject(sourceMeta);
  const raw = meta?.sourceMessageId;
  if (typeof raw === 'string' && /^\d+$/.test(raw.trim())) return raw.trim();
  if (typeof raw === 'number' && Number.isFinite(raw) && raw > 0) return String(Math.floor(raw));
  if (typeof raw === 'bigint' && raw > 0n) return raw.toString();
  return null;
}

// ? dedupe Group Tasks By Source ID ???????????????????
function dedupeGroupTasksBySourceId<T extends {
  id: bigint;
  mediaAsset: {
    id: bigint;
    status: MediaStatus;
    telegramFileId: string | null;
    dispatchMediaType: DispatchMediaType | null;
    sourceMeta: unknown;
  };
}>(tasks: T[]) {
  // ?? key Of ?????????????????????
  const keyOf = (task: T) => {
    const sourceMessageId = parseSourceMessageIdFromMeta(task.mediaAsset.sourceMeta);
    // 方案4：业务兜底去重键优先使用 sourceMessageId（即 source_id）；缺失时退化到 mediaAssetId。
    return sourceMessageId ? `src:${sourceMessageId}` : `asset:${task.mediaAsset.id.toString()}`;
  };

  // ?? score Of ?????????????????????
  const scoreOf = (task: T) => {
    const uploadedReady =
      task.mediaAsset.status === MediaStatus.relay_uploaded &&
      Boolean(task.mediaAsset.telegramFileId) &&
      Boolean(task.mediaAsset.dispatchMediaType);
    if (uploadedReady) return 3;
    if (task.mediaAsset.telegramFileId) return 2;
    return 1;
  };

  const picked = new Map<string, T>();
  const dropped: Array<{ key: string; taskId: string; mediaAssetId: string }> = [];

  for (const task of tasks) {
    const key = keyOf(task);
    const current = picked.get(key);
    if (!current) {
      picked.set(key, task);
      continue;
    }

    const scoreA = scoreOf(current);
    const scoreB = scoreOf(task);
    const shouldReplace = scoreB > scoreA || (scoreB === scoreA && task.id > current.id);

    if (shouldReplace) {
      dropped.push({ key, taskId: current.id.toString(), mediaAssetId: current.mediaAsset.id.toString() });
      picked.set(key, task);
    } else {
      dropped.push({ key, taskId: task.id.toString(), mediaAssetId: task.mediaAsset.id.toString() });
    }
  }

  return {
    deduped: Array.from(picked.values()),
    dropped,
    uniqueCount: picked.size,
  };
}

// ?? parse Collection Source Meta ????????????????????????
function parseCollectionSourceMeta(sourceMeta: unknown) {
  const meta = getSourceMetaObject(sourceMeta);
  if (meta?.isCollection !== true) {
    return { isCollection: false as const, collectionName: null, episodeNo: null };
  }

  const collectionName = typeof meta.collectionName === 'string' ? meta.collectionName.trim() : '';
  const episodeNo =
    typeof meta.episodeNo === 'number'
      ? meta.episodeNo
      : typeof meta.episodeNo === 'string' && /^\d+$/.test(meta.episodeNo)
        ? Number(meta.episodeNo)
        : null;

  return {
    isCollection: true as const,
    collectionName: collectionName || null,
    episodeNo,
  };
}

// ?? delete Catalog Source Item From Single ???????????????????????
async function deleteCatalogSourceItemFromSingle(args: {
  channelId: bigint;
  telegramMessageId: number;
}) {
  const deleted = await withPrismaRetry(
    () =>
      (prisma as any).catalogSourceItem.deleteMany({
        where: {
          channelId: args.channelId,
          telegramMessageId: BigInt(args.telegramMessageId),
        },
      }),
    { label: 'dispatch.deleteCatalogSourceItemFromSingle' },
  );

  const deletedCount = Number(deleted?.count || 0);
  if (deletedCount > 0) {
    catalogSourceWriteMetrics.deletedCollectionTotal += deletedCount;
  }
  return deletedCount;
}

// ?? delete Catalog Source Item From Group ???????????????????????
async function deleteCatalogSourceItemFromGroup(args: {
  channelId: bigint;
  groupKey: string;
  telegramMessageId: number | null;
}) {
  const deleted = await withPrismaRetry(
    () =>
      (prisma as any).catalogSourceItem.deleteMany({
        where: {
          channelId: args.channelId,
          OR: [
            { groupKey: args.groupKey },
            ...(args.telegramMessageId ? [{ telegramMessageId: BigInt(args.telegramMessageId) }] : []),
          ],
        },
      }),
    { label: 'dispatch.deleteCatalogSourceItemFromGroup' },
  );

  const deletedCount = Number(deleted?.count || 0);
  if (deletedCount > 0) {
    catalogSourceWriteMetrics.deletedCollectionTotal += deletedCount;
  }
  return deletedCount;
}

// ??????? upsert Catalog Source Item From Single ?????????????????
async function upsertCatalogSourceItemFromSingle(args: {
  channelId: bigint;
  dispatchTaskId: bigint;
  telegramMessageId: number;
  telegramMessageLink: string | null;
  caption: string | null;
  title: string | null;
}) {
  const startedAt = Date.now();
  catalogSourceWriteMetrics.upsertTotal += 1;
  catalogSourceWriteMetrics.upsertSingleTotal += 1;

  try {
    const existing = await withPrismaRetry(
      () =>
        (prisma as any).catalogSourceItem.findUnique({
          where: {
            channelId_telegramMessageId: {
              channelId: args.channelId,
              telegramMessageId: BigInt(args.telegramMessageId),
            },
          },
          select: { id: true },
        }),
      { label: 'dispatch.upsertCatalogSourceItemFromSingle.findExisting' },
    );

    await withPrismaRetry(
      () =>
        (prisma as any).catalogSourceItem.upsert({
          where: {
            channelId_telegramMessageId: {
              channelId: args.channelId,
              telegramMessageId: BigInt(args.telegramMessageId),
            },
          },
          update: {
            telegramMessageLink: args.telegramMessageLink,
            sourceType: 'single',
            title: args.title,
            caption: args.caption,
            sourceDispatchTaskId: args.dispatchTaskId,
            publishedAt: new Date(),
          },
          create: {
            channelId: args.channelId,
            telegramMessageId: BigInt(args.telegramMessageId),
            telegramMessageLink: args.telegramMessageLink,
            sourceType: 'single',
            title: args.title,
            caption: args.caption,
            sourceDispatchTaskId: args.dispatchTaskId,
            publishedAt: new Date(),
          },
        }),
      { label: 'dispatch.upsertCatalogSourceItemFromSingle' },
    );

    if (existing) {
      catalogSourceWriteMetrics.dedupHitTotal += 1;
    }

    catalogSourceWriteMetrics.upsertSuccessTotal += 1;
  } catch (error) {
    catalogSourceWriteMetrics.upsertFailedTotal += 1;
    throw error;
  } finally {
    catalogSourceWriteMetrics.upsertDurationMsTotal += Date.now() - startedAt;
  }
}

// ??????? upsert Catalog Source Item From Group ?????????????????
async function upsertCatalogSourceItemFromGroup(args: {
  channelId: bigint;
  seedDispatchTaskId: bigint;
  groupKey: string;
  telegramMessageId: number;
  telegramMessageLink: string | null;
  caption: string | null;
  title: string | null;
}) {
  const startedAt = Date.now();
  catalogSourceWriteMetrics.upsertTotal += 1;
  catalogSourceWriteMetrics.upsertGroupTotal += 1;

  try {
    const existing = await withPrismaRetry(
      () =>
        (prisma as any).catalogSourceItem.findUnique({
          where: {
            channelId_telegramMessageId: {
              channelId: args.channelId,
              telegramMessageId: BigInt(args.telegramMessageId),
            },
          },
          select: { id: true },
        }),
      { label: 'dispatch.upsertCatalogSourceItemFromGroup.findExisting' },
    );

    await withPrismaRetry(
      () =>
        (prisma as any).catalogSourceItem.upsert({
          where: {
            channelId_telegramMessageId: {
              channelId: args.channelId,
              telegramMessageId: BigInt(args.telegramMessageId),
            },
          },
          update: {
            telegramMessageLink: args.telegramMessageLink,
            sourceType: 'group',
            groupKey: args.groupKey,
            title: args.title,
            caption: args.caption,
            sourceDispatchTaskId: args.seedDispatchTaskId,
            publishedAt: new Date(),
          },
          create: {
            channelId: args.channelId,
            telegramMessageId: BigInt(args.telegramMessageId),
            telegramMessageLink: args.telegramMessageLink,
            sourceType: 'group',
            groupKey: args.groupKey,
            title: args.title,
            caption: args.caption,
            sourceDispatchTaskId: args.seedDispatchTaskId,
            publishedAt: new Date(),
          },
        }),
      { label: 'dispatch.upsertCatalogSourceItemFromGroup' },
    );

    if (existing) {
      catalogSourceWriteMetrics.dedupHitTotal += 1;
    }

    catalogSourceWriteMetrics.upsertSuccessTotal += 1;
  } catch (error) {
    catalogSourceWriteMetrics.upsertFailedTotal += 1;
    throw error;
  } finally {
    catalogSourceWriteMetrics.upsertDurationMsTotal += Date.now() - startedAt;
  }
}

// ???? TypeB ???????????? sendMediaGroup ??????????????
export async function handleDispatchGroupJob(
  dispatchTaskIdRaw: string,
  jobId: string,
  attemptsMade: number,
) {
  const dispatchTaskId = BigInt(dispatchTaskIdRaw);

  const seedTask = await withPrismaRetry(
    () =>
      prisma.dispatchTask.findUnique({
        where: { id: dispatchTaskId },
        include: {
          channel: {
            select: {
              id: true,
              tgChatId: true,
              defaultBotId: true,
            },
          },
          mediaAsset: {
            select: {
              id: true,
              telegramFileId: true,
              dispatchMediaType: true,
              sourceMeta: true,
            },
          },
        },
      }),
    { label: 'dispatch.handleDispatchGroupJob.findSeedTask' },
  );

  if (!seedTask || !seedTask.groupKey) {
    throw new Error('group worker 仅处理 grouped 任务');
  }

  logger.info('[typeb_group] dispatch-send-group started', {
    dispatchTaskId: dispatchTaskIdRaw,
    seedTaskId: seedTask.id.toString(),
    channelId: seedTask.channelId.toString(),
    groupKey: seedTask.groupKey,
    seedTaskStatus: seedTask.status,
    attemptsMade,
    jobId,
  });

  const groupTasks = await withPrismaRetry(
    () =>
      prisma.dispatchTask.findMany({
        where: {
          channelId: seedTask.channelId,
          groupKey: seedTask.groupKey,
        },
        orderBy: [{ scheduleSlot: 'asc' }, { id: 'asc' }],
        include: {
          mediaAsset: {
            select: {
              id: true,
              status: true,
              telegramFileId: true,
              dispatchMediaType: true,
              sourceMeta: true,
              localPath: true,
            },
          },
        },
      }),
    { label: 'dispatch.handleDispatchGroupJob.findGroupTasks' },
  );

  let groupTaskState = await withPrismaRetry(
    () =>
      (prisma as any).dispatchGroupTask.findFirst({
        where: {
          channelId: seedTask.channelId,
          groupKey: seedTask.groupKey,
        },
        orderBy: [{ updatedAt: 'desc' }],
        select: {
          id: true,
          status: true,
          expectedMediaCount: true,
          sourceExpectedCount: true,
          actualReadyCount: true,
          actualUploadedCount: true,
          lastArrivalAt: true,
          sealedAt: true,
          sealReason: true,
        },
      }),
    { label: 'dispatch.handleDispatchGroupJob.findDispatchGroupTaskState' },
  );

  const now = new Date();
  const readyStatuses = new Set<TaskStatus>([
    TaskStatus.pending,
    TaskStatus.scheduled,
    TaskStatus.failed,
    TaskStatus.running,
  ]);

  const recoverableDeadTasks = groupTasks.filter((t) => {
    if (t.status !== TaskStatus.dead) return false;
    if (!t.readyDeadlineAt) return false;
    return t.readyDeadlineAt.getTime() > now.getTime();
  });

  if (recoverableDeadTasks.length > 0) {
    await withPrismaRetry(
      () =>
        prisma.dispatchTask.updateMany({
          where: {
            id: { in: recoverableDeadTasks.map((t) => t.id) },
          },
          data: {
            status: TaskStatus.scheduled,
            nextRunAt: now,
            telegramErrorCode: null,
            telegramErrorMessage: null,
            finishedAt: null,
          },
        }),
      { label: 'dispatch.handleDispatchGroupJob.recoverDeadTasksWithinDeadline' },
    );
  }

  const normalizedGroupTasks = groupTasks.map((t) =>
    recoverableDeadTasks.some((r) => r.id === t.id)
      ? {
          ...t,
          status: TaskStatus.scheduled,
          nextRunAt: now,
          telegramErrorCode: null,
          telegramErrorMessage: null,
          finishedAt: null,
        }
      : t,
  );

  // 方案4：组消息组装前做业务层去重聚合，避免重复 source_id 被重复发送。
  const dedupeSnapshot = dedupeGroupTasksBySourceId(normalizedGroupTasks as any);
  const groupedTasksDeduped = dedupeSnapshot.deduped as typeof normalizedGroupTasks;

  const alreadySuccess = groupedTasksDeduped.filter((t) => t.status === TaskStatus.success);
  const pendingGroupTasks = groupedTasksDeduped.filter((t) => readyStatuses.has(t.status));
  const excludedGroupTasks = groupedTasksDeduped.filter(
    (t) => !readyStatuses.has(t.status) && t.status !== TaskStatus.success,
  );
  const uploadedReadyTasks = groupedTasksDeduped.filter(
    (t) =>
      t.mediaAsset.status === MediaStatus.relay_uploaded &&
      Boolean(t.mediaAsset.telegramFileId) &&
      Boolean(t.mediaAsset.dispatchMediaType),
  );
  // 修复点：为避免“sourceExpectedCount 缺失导致永久阻塞”，
  // 在 worker 预检阶段再从组内 mediaAsset.sourceMeta 聚合一次候选值。
  const sourceExpectedCountFromAssets = normalizedGroupTasks
    .map((t) => parseSourceExpectedCountFromMeta(t.mediaAsset?.sourceMeta))
    .filter((n): n is number => typeof n === 'number' && n > 0);
  const computedSourceExpectedCount =
    sourceExpectedCountFromAssets.length > 0 ? Math.max(...sourceExpectedCountFromAssets) : 0;

  // 仅当 dispatchGroupTask 当前值缺失(<=0)时才补写，避免覆盖已有正确值。
  // 这里使用 updateMany + 条件 where，确保并发场景下是幂等/保守补写。
  if (groupTaskState?.id && Number(groupTaskState.sourceExpectedCount ?? 0) <= 0 && computedSourceExpectedCount > 0) {
    await withPrismaRetry(
      () =>
        (prisma as any).dispatchGroupTask.updateMany({
          where: {
            id: groupTaskState.id,
            OR: [{ sourceExpectedCount: null }, { sourceExpectedCount: { lte: 0 } }],
          },
          data: {
            sourceExpectedCount: computedSourceExpectedCount,
          },
        }),
      { label: 'dispatch.handleDispatchGroupJob.backfillSourceExpectedCount' },
    );

    logger.info('[typeb_group][verify] sourceExpectedCount backfilled from grouped assets', {
      dispatchTaskId: dispatchTaskIdRaw,
      groupKey: seedTask.groupKey,
      dispatchGroupTaskId: groupTaskState.id.toString(),
      computedSourceExpectedCount,
      sourceFrom: 'grouped_media_asset_source_meta',
    });

    groupTaskState = await withPrismaRetry(
      () =>
        (prisma as any).dispatchGroupTask.findFirst({
          where: {
            channelId: seedTask.channelId,
            groupKey: seedTask.groupKey,
          },
          orderBy: [{ updatedAt: 'desc' }],
          select: {
            id: true,
            status: true,
            expectedMediaCount: true,
            sourceExpectedCount: true,
            actualReadyCount: true,
            actualUploadedCount: true,
            lastArrivalAt: true,
            sealedAt: true,
            sealReason: true,
          },
        }),
      { label: 'dispatch.handleDispatchGroupJob.refetchDispatchGroupTaskStateAfterBackfill' },
    );
  }

  // 最终读取采用 max(db值, 本轮聚合值)：
  // - 不放宽分组闸门规则（仍要求 sourceExpectedCount>0）
  // - 但可消除“db暂未补写完成”带来的短暂读空窗。
  const sourceExpectedCount = Math.max(
    Number(groupTaskState?.sourceExpectedCount ?? 0),
    computedSourceExpectedCount,
  );
  const expectedTotalSource = sourceExpectedCount > 0
    ? Number(groupTaskState?.sourceExpectedCount ?? 0) > 0
      ? 'source_expected_count'
      : 'source_expected_count_backfilled_from_assets'
    : 'fallback_missing_source_expected_count';
  const expectedCount = sourceExpectedCount;
  const expectedUniqueCount = expectedCount;
  const actualUniqueCount = dedupeSnapshot.uniqueCount;
  const lastArrivalAt = groupTaskState?.lastArrivalAt ? new Date(groupTaskState.lastArrivalAt) : null;
  const quietReached =
    Boolean(lastArrivalAt) &&
    now.getTime() - (lastArrivalAt as Date).getTime() >= TYPEB_GROUP_SEAL_QUIET_PERIOD_MS;

  let sealedAt = groupTaskState?.sealedAt ? new Date(groupTaskState.sealedAt) : null;
  let sealReason = groupTaskState?.sealReason ?? null;

  if (!sealedAt && quietReached && groupTaskState?.id && lastArrivalAt) {
    await withPrismaRetry(
      () =>
        (prisma as any).dispatchGroupTask.updateMany({
          where: {
            id: groupTaskState.id,
            sealedAt: null,
            lastArrivalAt: {
              lte: new Date(now.getTime() - TYPEB_GROUP_SEAL_QUIET_PERIOD_MS),
            },
          },
          data: {
            sealedAt: now,
            sealReason: 'quiet_period_elapsed_before_send',
          },
        }),
      { label: 'dispatch.handleDispatchGroupJob.sealBeforeSendByQuietPeriodCas' },
    );

    const sealedState = await withPrismaRetry(
      () =>
        (prisma as any).dispatchGroupTask.findUnique({
          where: { id: groupTaskState.id },
          select: {
            sealedAt: true,
            sealReason: true,
          },
        }),
      { label: 'dispatch.handleDispatchGroupJob.refetchSealState' },
    );

    sealedAt = sealedState?.sealedAt ? new Date(sealedState.sealedAt) : null;
    sealReason = sealedState?.sealReason ?? null;
  }

  const sealedEnough = Boolean(sealedAt);
  const hasSourceExpectedCount = expectedCount > 0;
  const readyEnough = hasSourceExpectedCount && pendingGroupTasks.length >= expectedCount;
  const uploadedEnough = hasSourceExpectedCount && uploadedReadyTasks.length >= expectedCount;

  logger.info('[typeb_group] dispatch-send-group preflight snapshot', {
    dispatchTaskId: dispatchTaskIdRaw,
    groupKey: seedTask.groupKey,
    channelId: seedTask.channelId.toString(),
    dispatchGroupTaskId: groupTaskState?.id?.toString?.() ?? null,
    expectedCount,
    expectedUniqueCount,
    actualUniqueCount,
    readyCount: pendingGroupTasks.length,
    uploadedCount: uploadedReadyTasks.length,
    successCount: alreadySuccess.length,
    recoveredDeadTaskIds: recoverableDeadTasks.map((t) => t.id.toString()),
    taskIds: groupedTasksDeduped.map((t) => t.id.toString()),
    assetIds: groupedTasksDeduped.map((t) => t.mediaAsset.id.toString()),
    dedupeDropped: dedupeSnapshot.dropped,
    statuses: groupedTasksDeduped.map((t) => ({
      taskId: t.id.toString(),
      status: t.status,
      retryCount: t.retryCount,
      maxRetries: t.maxRetries,
      nextRunAt: t.nextRunAt?.toISOString?.() ?? null,
      readyDeadlineAt: t.readyDeadlineAt?.toISOString?.() ?? null,
      telegramErrorCode: t.telegramErrorCode ?? null,
      telegramErrorMessage: t.telegramErrorMessage ?? null,
    })),
    excludedTasks: excludedGroupTasks.map((t) => ({
      taskId: t.id.toString(),
      status: t.status,
      readyDeadlineAt: t.readyDeadlineAt?.toISOString?.() ?? null,
      telegramErrorCode: t.telegramErrorCode ?? null,
      telegramErrorMessage: t.telegramErrorMessage ?? null,
    })),
    mediaSources: groupedTasksDeduped.map((t) => {
      const meta = t.mediaAsset.sourceMeta && typeof t.mediaAsset.sourceMeta === 'object'
        ? (t.mediaAsset.sourceMeta as Record<string, unknown>)
        : null;
      return {
        taskId: t.id.toString(),
        assetId: t.mediaAsset.id.toString(),
        telegramFileIdPresent: Boolean(t.mediaAsset.telegramFileId),
        dispatchMediaType: t.mediaAsset.dispatchMediaType ?? null,
        relayResolvedMediaType: typeof meta?.relayResolvedMediaType === 'string' ? meta.relayResolvedMediaType : null,
        mimeType: typeof meta?.mimeType === 'string' ? meta.mimeType : null,
        localPath: typeof meta?.localPath === 'string' ? meta.localPath : null,
      };
    }),
    lastArrivalAt: lastArrivalAt?.toISOString() ?? null,
    quietPeriodMs: TYPEB_GROUP_SEAL_QUIET_PERIOD_MS,
    quietReached,
    sealedAt: sealedAt?.toISOString() ?? null,
    sealReason,
    readyAssert: 'ready_count_eq_expected_or_wait',
    uploadedAssert: 'blocked_until_all_uploaded',
    blockedReason: !sealedEnough || !readyEnough || !uploadedEnough ? 'blocked_until_all_uploaded' : null,
  });

  if (pendingGroupTasks.length === 0 && alreadySuccess.length > 0) {
    logger.info('[typeb_metrics] group send skipped already success', {
      typeb_group_send_fallback_single_total: 0,
      dispatchTaskId: dispatchTaskIdRaw,
      groupKey: seedTask.groupKey,
      groupedTaskCount: groupTasks.length,
    });
    return {
      ok: true,
      grouped: true,
      skipped: true,
      reason: 'group_already_success',
      dispatchTaskId: dispatchTaskIdRaw,
      groupKey: seedTask.groupKey,
    };
  }

  if (!hasSourceExpectedCount) {
    logger.error('[typeb_group][assert] source_expected_count missing in dispatch worker preflight, block send', {
      dispatchTaskId: dispatchTaskIdRaw,
      groupKey: seedTask.groupKey,
      channelId: seedTask.channelId.toString(),
      dispatchGroupTaskId: groupTaskState?.id?.toString?.() ?? null,
      expectedCount,
      expectedTotalSource,
      sourceExpectedCount,
      fallbackExpectedMediaCount: Number(groupTaskState?.expectedMediaCount ?? 0),
      readyCount: pendingGroupTasks.length,
      uploadedCount: uploadedReadyTasks.length,
      lastArrivalAt: lastArrivalAt?.toISOString() ?? null,
      quietPeriodMs: TYPEB_GROUP_SEAL_QUIET_PERIOD_MS,
      quietReached,
      sealedAt: sealedAt?.toISOString() ?? null,
      sealReason,
      blockedReason: 'source_expected_count_missing',
      action: 'reschedule_without_failure',
      retryAfterMs: TYPEB_GROUP_RETRY_CHECK_MS,
    });
  }

  const uniqueCountMatched = expectedUniqueCount > 0 && actualUniqueCount === expectedUniqueCount;

  if (!uniqueCountMatched) {
    logger.error('[typeb_group][assert] unique count mismatch, block send and alert', {
      dispatchTaskId: dispatchTaskIdRaw,
      groupKey: seedTask.groupKey,
      channelId: seedTask.channelId.toString(),
      expectedUniqueCount,
      actualUniqueCount,
      dedupeDropped: dedupeSnapshot.dropped,
      blockedReason: 'expected_unique_count_not_equal_actual_unique_count',
      action: 'reschedule_without_failure',
      retryAfterMs: TYPEB_GROUP_RETRY_CHECK_MS,
    });
  }

  if (!sealedEnough || !readyEnough || !uploadedEnough || !hasSourceExpectedCount || !uniqueCountMatched) {
    logger.warn('[typeb_group][diag] preflight blocked snapshot', {
      dispatchTaskId: dispatchTaskIdRaw,
      groupKey: seedTask.groupKey,
      channelId: seedTask.channelId.toString(),
      dispatchGroupTaskId: groupTaskState?.id?.toString?.() ?? null,
      sealedEnough,
      readyEnough,
      uploadedEnough,
      expectedCount,
      expectedTotalSource,
      readyCount: pendingGroupTasks.length,
      uploadedCount: uploadedReadyTasks.length,
      excludedCount: excludedGroupTasks.length,
      lastArrivalAt: lastArrivalAt?.toISOString() ?? null,
      quietPeriodMs: TYPEB_GROUP_SEAL_QUIET_PERIOD_MS,
      quietReached,
      sealedAt: sealedAt?.toISOString() ?? null,
      sealReason,
      blockedReason: 'blocked_until_all_uploaded',
      action: 'reschedule_without_failure',
      retryAfterMs: TYPEB_GROUP_RETRY_CHECK_MS,
    });

    await withPrismaRetry(
      () =>
        prisma.dispatchTask.updateMany({
          where: {
            id: { in: pendingGroupTasks.map((t) => t.id) },
            status: { in: [TaskStatus.pending, TaskStatus.scheduled, TaskStatus.failed, TaskStatus.running] },
          },
          data: {
            status: TaskStatus.scheduled,
            nextRunAt: new Date(Date.now() + TYPEB_GROUP_RETRY_CHECK_MS),
          },
        }),
      { label: 'dispatch.handleDispatchGroupJob.preflightBlockedRescheduleTasks' },
    );

    if (groupTaskState?.id) {
      await withPrismaRetry(
        () =>
          (prisma as any).dispatchGroupTask.update({
            where: { id: groupTaskState.id },
            data: {
              status: TaskStatus.scheduled,
              nextRunAt: new Date(Date.now() + TYPEB_GROUP_RETRY_CHECK_MS),
            },
          }),
        { label: 'dispatch.handleDispatchGroupJob.preflightBlockedRescheduleGroupTask' },
      );
    }

    return {
      ok: true,
      grouped: true,
      skipped: true,
      reason: 'group_not_fully_uploaded_waiting',
      dispatchTaskId: dispatchTaskIdRaw,
      groupKey: seedTask.groupKey,
      expectedCount,
      expectedTotalSource,
      readyCount: pendingGroupTasks.length,
      uploadedCount: uploadedReadyTasks.length,
    };
  }

  const sourceRelayBotIdRaw =
    (seedTask.mediaAsset.sourceMeta as Record<string, unknown> | null | undefined)?.relayBotId;
  const sourceRelayBotId =
    typeof sourceRelayBotIdRaw === 'string' && /^\d+$/.test(sourceRelayBotIdRaw)
      ? BigInt(sourceRelayBotIdRaw)
      : null;
  const resolvedBotId = sourceRelayBotId ?? seedTask.botId ?? seedTask.channel.defaultBotId;

  if (!resolvedBotId) {
    throw new Error('分发任务或频道未配置机器人');
  }

  const bot = await withPrismaRetry(
    () =>
      prisma.bot.findFirst({
        where: {
          id: resolvedBotId,
          status: 'active',
        },
        select: { id: true, tokenEncrypted: true },
      }),
    { label: 'dispatch.handleDispatchGroupJob.findActiveBot' },
  );

  if (!bot) {
    throw new Error(`未找到可用机器人: dispatchTaskId=${dispatchTaskIdRaw}`);
  }

  const pendingWithMeta = pendingGroupTasks.map((t) => {
    const meta = t.mediaAsset.sourceMeta && typeof t.mediaAsset.sourceMeta === 'object'
      ? (t.mediaAsset.sourceMeta as Record<string, unknown>)
      : null;

    const persistedDispatchType =
      t.mediaAsset.dispatchMediaType === DispatchMediaType.photo
        ? ('photo' as const)
        : t.mediaAsset.dispatchMediaType === DispatchMediaType.video
          ? ('video' as const)
          : null;

    const resolvedByMeta = resolveMediaTypeByMeta(meta, null);
    const resolvedType = persistedDispatchType ?? resolvedByMeta;

    return {
      task: t,
      meta,
      persistedDispatchType,
      resolvedByMeta,
      resolvedType,
    };
  });

  const unresolvedTypeCount = pendingWithMeta.filter(({ resolvedType }) => !resolvedType).length;

  if (unresolvedTypeCount > 0) {
    const unresolvedItems = pendingWithMeta
      .filter(({ resolvedType }) => !resolvedType)
      .map(({ task, meta }) => ({
        taskId: task.id.toString(),
        mediaAssetId: task.mediaAsset.id.toString(),
        telegramFileIdPresent: Boolean(task.mediaAsset.telegramFileId),
        relayResolvedMediaType: typeof meta?.relayResolvedMediaType === 'string' ? meta.relayResolvedMediaType : null,
        mimeType: typeof meta?.mimeType === 'string' ? meta.mimeType : null,
        localPath: typeof meta?.localPath === 'string' ? meta.localPath : null,
      }));

    logger.warn('[typeb_metrics] group preflight rejected: unresolved_media_type', {
      typeb_group_preflight_reject_total: 1,
      dispatchTaskId: dispatchTaskIdRaw,
      groupKey: seedTask.groupKey,
      unresolvedTypeCount,
      unresolvedItems,
      reason: 'dispatch_media_type_missing_and_meta_unresolvable',
    });

    throw new Error(`group_preflight_failed_unresolved_type: count=${unresolvedTypeCount}`);
  }

  const enriched = pendingWithMeta.map(({ task, meta, resolvedType }) => {
    const mediaType = resolvedType as 'photo' | 'video';
    const dims = readMediaDimensions(meta);
    return {
      task,
      mediaType,
      meta,
      ...dims,
      orientation: mediaType === 'photo' ? classifyImageOrientation(dims.aspectRatio) : null,
    };
  });

  const photos = enriched.filter((x) => x.mediaType === 'photo');
  const videos = enriched.filter((x) => x.mediaType === 'video');

  let sortedMedia = enriched;

  if (photos.length === 1 && videos.length === 1) {
    // 1图1视频：依据图片比例决定“横向优先/纵向优先”，实现上仍是图在前，再视频
    const photo = photos[0];
    const video = videos[0];
    sortedMedia = [photo, video];

    const layoutHint =
      photo.aspectRatio && photo.aspectRatio >= 1.2
        ? 'left_photo_right_video'
        : photo.aspectRatio && photo.aspectRatio <= 0.85
          ? 'top_photo_bottom_video'
          : 'side_by_side_preferred';

    logger.info('[typeb_metrics] grouped media layout hint', {
      dispatchTaskId: dispatchTaskIdRaw,
      groupKey: seedTask.groupKey,
      layoutHint,
      photoAspectRatio: photo.aspectRatio,
    });
  } else {
    // 多图多视频：视频在图片之后；图片按尺寸自由排列（横->方->竖，组内面积降序）
    const photoSorted = [...photos].sort((a, b) => {
      // ?? rank ?????????????????????
      const rank = (o: string | null) => (o === 'landscape' ? 0 : o === 'square' ? 1 : 2);
      const r = rank(a.orientation) - rank(b.orientation);
      if (r !== 0) return r;
      const areaA = a.area ?? 0;
      const areaB = b.area ?? 0;
      if (areaA !== areaB) return areaB - areaA;
      return String(a.task.mediaAsset.id).localeCompare(String(b.task.mediaAsset.id));
    });

    const videoSorted = [...videos].sort((a, b) =>
      String(a.task.mediaAsset.id).localeCompare(String(b.task.mediaAsset.id)),
    );

    sortedMedia = [...photoSorted, ...videoSorted];
  }

  const sortedGroupTasks = sortedMedia.map((x) => x.task);

  const media = sortedMedia
    .map((x, idx) => ({
      idx,
      taskId: x.task.id,
      telegramFileId: x.task.mediaAsset.telegramFileId,
      type: x.mediaType,
      meta: x.meta,
    }))
    .filter((x) => Boolean(x.telegramFileId));

  if (media.length < 1) {
    throw new Error(`group_not_ready_media: mediaCount=${media.length}`);
  }

  const fallbackSingleMode = media.length === 1;
  const { caption, captionSource } = buildGroupCaptionFromTasks(sortedGroupTasks as any);

  // ?? build Input Media ?????????????????????
  const buildInputMedia = (overrideTypeByTaskId?: Map<string, 'photo' | 'video'>) =>
    media.map((item, index) => ({
      type: overrideTypeByTaskId?.get(item.taskId.toString()) ?? item.type,
      media: item.telegramFileId,
      ...(index === 0 && caption ? { caption } : {}),
    }));

  logger.info('[typeb_metrics] group send attempt', {
    typeb_group_send_attempt_total: 1,
    dispatchTaskId: dispatchTaskIdRaw,
    groupKey: seedTask.groupKey,
    mediaCount: media.length,
    fallbackSingleMode,
    captionSource,
    unresolvedTypeCount,
    mediaTypeMappingAssert: 'meta_or_mime_or_ext_single_resolver',
    mediaTypeMapping: sortedMedia.map((m) => ({
      taskId: m.task.id.toString(),
      assetId: m.task.mediaAsset.id.toString(),
      persistedDispatchMediaType: m.task.mediaAsset.dispatchMediaType ?? null,
      relayResolvedMediaType: typeof m.meta?.relayResolvedMediaType === 'string' ? m.meta.relayResolvedMediaType : null,
      mimeType: typeof m.meta?.mimeType === 'string' ? m.meta.mimeType : null,
      localPath: typeof m.meta?.localPath === 'string' ? m.meta.localPath : null,
      telegramType: m.mediaType,
    })),
  });

  try {
    let sendResult;
    let correctedByRetry = false;

    if (fallbackSingleMode) {
      const only = media[0];
      // ?? send Single ?????????????????????
      const sendSingle = async (method: 'sendPhoto' | 'sendVideo', parseMode: string | null | undefined) => {
        if (method === 'sendPhoto') {
          return sendPhotoByTelegram({
            botToken: bot.tokenEncrypted,
            chatId: seedTask.channel.tgChatId,
            fileId: only.telegramFileId as string,
            caption,
            parseMode,
            replyMarkup: seedTask.replyMarkup ?? seedTask.channel.aiReplyMarkup ?? undefined,
          });
        }

        return sendVideoByTelegram({
          botToken: bot.tokenEncrypted,
          chatId: seedTask.channel.tgChatId,
          fileId: only.telegramFileId as string,
          caption,
          parseMode,
          replyMarkup: seedTask.replyMarkup ?? seedTask.channel.aiReplyMarkup ?? undefined,
        });
      };

      const singleMethod = only.type === 'photo' ? 'sendPhoto' : 'sendVideo';
      logger.info('[typeb_group] fallback to single send for one-item grouped task', {
        typeb_group_send_fallback_single_total: 1,
        dispatchTaskId: dispatchTaskIdRaw,
        groupKey: seedTask.groupKey,
        mediaCount: media.length,
        singleMethod,
      });

      try {
        sendResult = await sendSingle(singleMethod, seedTask.parseMode);
      } catch (sendError) {
        const errorObj = sendError as TelegramError;

        if (singleMethod === 'sendVideo' && isPhotoAsVideoError({ code: errorObj.code, message: errorObj.message })) {
          logger.warn('[typeb_group] single fallback sendVideo failed by photo type, retry with sendPhoto', {
            dispatchTaskId: dispatchTaskIdRaw,
            groupKey: seedTask.groupKey,
            errorCode: errorObj.code,
            errorMessage: errorObj.message,
          });
          sendResult = await sendSingle('sendPhoto', seedTask.parseMode);
        } else if (
          seedTask.parseMode?.toUpperCase() === 'HTML' &&
          isParseEntitiesError({ code: errorObj.code, message: errorObj.message })
        ) {
          logger.warn('[typeb_group] single fallback parse entities failed, retry with plain text', {
            dispatchTaskId: dispatchTaskIdRaw,
            groupKey: seedTask.groupKey,
            singleMethod,
            errorCode: errorObj.code,
            errorMessage: errorObj.message,
          });
          sendResult = await sendSingle(singleMethod, null);
        } else {
          throw sendError;
        }
      }
    } else {
      try {
        sendResult = await sendTelegramRequest({
          botToken: bot.tokenEncrypted,
          method: 'sendMediaGroup',
          payload: {
            chat_id: seedTask.channel.tgChatId,
            media: JSON.stringify(buildInputMedia()),
          },
        });
      } catch (sendError) {
        const errorObj = sendError as TelegramError;
        if (isVideoAsPhotoError({ code: errorObj.code, message: errorObj.message })) {
          const retryTypes = new Map<string, 'photo' | 'video'>();
          const correctedTaskIds: string[] = [];

          for (const item of sortedMedia) {
            const taskId = item.task.id.toString();
            const persisted = item.task.mediaAsset.dispatchMediaType;
            if (persisted === DispatchMediaType.video && item.mediaType !== 'video') {
              retryTypes.set(taskId, 'video');
              correctedTaskIds.push(taskId);
            }
          }

          if (correctedTaskIds.length === 0) {
            logger.warn('[typeb_metrics] group type mismatch detected but no targeted correction candidate', {
              typeb_group_type_mismatch_total: 1,
              dispatchTaskId: dispatchTaskIdRaw,
              groupKey: seedTask.groupKey,
              errorCode: errorObj.code,
              errorMessage: errorObj.message,
            });
            throw sendError;
          }

          logger.warn('[typeb_group] sendMediaGroup 检测到 Video 被当作 Photo，执行定向纠错重试', {
            typeb_group_retry_corrected_total: 1,
            dispatchTaskId: dispatchTaskIdRaw,
            groupKey: seedTask.groupKey,
            mediaCount: media.length,
            correctedTaskIds,
            retryMode: 'targeted_by_persisted_dispatch_media_type',
            errorCode: errorObj.code,
            errorMessage: errorObj.message,
          });

          correctedByRetry = true;
          sendResult = await sendTelegramRequest({
            botToken: bot.tokenEncrypted,
            method: 'sendMediaGroup',
            payload: {
              chat_id: seedTask.channel.tgChatId,
              media: JSON.stringify(buildInputMedia(retryTypes)),
            },
          });
        } else {
          throw sendError;
        }
      }
    }

    const firstMessageId = sendResult.messageIds?.[0] ?? sendResult.messageId;

    const groupMessageLink = firstMessageId
      ? `https://t.me/c/${seedTask.channel.tgChatId.replace('-100', '')}/${firstMessageId}`
      : null;

    await withPrismaRetry(
      () =>
        prisma.$transaction([
          ...sortedGroupTasks.map((t) =>
            prisma.dispatchTask.update({
              where: { id: t.id },
              data: {
                status: TaskStatus.success,
                finishedAt: new Date(),
                botId: bot.id,
                telegramMessageId: firstMessageId ? BigInt(firstMessageId) : undefined,
                telegramMessageLink: groupMessageLink,
                telegramErrorCode: null,
                telegramErrorMessage: null,
              },
            }),
          ),
          prisma.channel.update({
            where: { id: seedTask.channelId },
            data: { lastPostAt: new Date() },
          }),
        ]),
      { label: 'dispatch.handleDispatchGroupJob.markGroupSuccess' },
    );

    const hasCollectionSource = sortedGroupTasks.some(
      (task) => parseCollectionSourceMeta(task.mediaAsset?.sourceMeta).isCollection,
    );

    if (firstMessageId && hasCollectionSource) {
      const deletedCount = await deleteCatalogSourceItemFromGroup({
        channelId: seedTask.channelId,
        groupKey: seedTask.groupKey,
        telegramMessageId: firstMessageId,
      });
      catalogSourceWriteMetrics.skippedCollectionTotal += 1;
      logger.info('[catalog_source_item] skip collection group projection', {
        dispatchTaskId: dispatchTaskIdRaw,
        channelId: seedTask.channelId.toString(),
        groupKey: seedTask.groupKey,
        telegramMessageId: String(firstMessageId),
        deletedCount,
      });
    } else if (firstMessageId) {
      await upsertCatalogSourceItemFromGroup({
        channelId: seedTask.channelId,
        seedDispatchTaskId: seedTask.id,
        groupKey: seedTask.groupKey,
        telegramMessageId: firstMessageId,
        telegramMessageLink: groupMessageLink,
        caption: caption || null,
        title: extractCatalogShortTitle(caption),
      });
    }

    logger.info('[typeb_metrics] group send success', {
      typeb_group_send_success_total: 1,
      dispatchTaskId: dispatchTaskIdRaw,
      groupKey: seedTask.groupKey,
      sentCount: media.length,
      mediaGroupId: sendResult.mediaGroupId ?? null,
      captionSource,
      correctedByRetry,
    });

    await cleanupDispatchScopedDirectoriesAfterSuccess(
      sortedGroupTasks as Array<{ mediaAsset: { sourceMeta: unknown; localPath?: string | null } }>,
      seedTask.groupKey ? new Set([seedTask.groupKey]) : undefined,
    );

    return {
      ok: true,
      grouped: true,
      dispatchTaskId: dispatchTaskIdRaw,
      groupKey: seedTask.groupKey,
      sentCount: media.length,
      firstMessageId,
      mediaGroupId: sendResult.mediaGroupId ?? null,
    };
  } catch (error) {
    const now = new Date();
    const errorObj = error as TelegramError;
    const message = errorObj.message || '未知组级分发错误';
    const code = errorObj.code || 'GROUP_DISPATCH_ERROR';

    let deadCount = 0;
    let failedCount = 0;

    for (const t of sortedGroupTasks) {
      const nextRetryCount = t.retryCount + 1;
      const retryAfterSec = errorObj.retryAfterSec;
      const fallbackBackoffSec = getBackoffSeconds(nextRetryCount);
      const finalBackoffSec = retryAfterSec ?? fallbackBackoffSec;
      const nextRunAt = new Date(Date.now() + finalBackoffSec * 1000);
      const deterministic = isDeterministicDispatchError({ code, message });
      const exceeded = nextRetryCount > t.maxRetries;
      const nextStatus = exceeded || deterministic ? TaskStatus.dead : TaskStatus.failed;

      if (nextStatus === TaskStatus.dead) deadCount += 1;
      else failedCount += 1;

      await withPrismaRetry(
        () =>
          prisma.dispatchTask.update({
            where: { id: t.id },
            data: {
              status: nextStatus,
              retryCount: nextRetryCount,
              nextRunAt: nextStatus === TaskStatus.dead ? t.nextRunAt : nextRunAt,
              telegramErrorCode: code,
              telegramErrorMessage: message,
              finishedAt: now,
            },
          }),
        { label: 'dispatch.handleDispatchGroupJob.markFailedOrDead' },
      );

      await withPrismaRetry(
        () =>
          prisma.dispatchTaskLog.create({
            data: {
              dispatchTaskId: t.id,
              action: nextStatus === TaskStatus.dead ? 'task_dead_group_send' : 'task_failed_group_send',
              detail: {
                groupKey: seedTask.groupKey,
                errorCode: code,
                errorMessage: message,
                retryCount: nextRetryCount,
                deterministic,
              },
            },
          }),
        { label: 'dispatch.handleDispatchGroupJob.logFailedOrDead' },
      );
    }

    logger.warn('[typeb_metrics] group send failed', {
      typeb_group_send_failed_total: failedCount > 0 ? 1 : 0,
      typeb_group_send_dead_total: deadCount > 0 ? 1 : 0,
      typeb_group_send_retry_total: failedCount,
      dispatchTaskId: dispatchTaskIdRaw,
      groupKey: seedTask.groupKey,
      errorCode: code,
      errorMessage: message,
      failedCount,
      deadCount,
    });

    throw error;
  }
}

// ???? TypeB ????????????????????????????
export async function handleDispatchJob(
  dispatchTaskIdRaw: string,
  jobId: string,
  attemptsMade: number,
) {
  const dispatchTaskId = BigInt(dispatchTaskIdRaw);

  const task = await withPrismaRetry(
    () =>
      prisma.dispatchTask.findUnique({
        where: { id: dispatchTaskId },
        include: {
          channel: {
            select: {
              id: true,
              name: true,
              tgChatId: true,
              defaultBotId: true,
              aiModelProfileId: true,
              aiSystemPromptTemplate: true,
              cloneUseAiPromptTemplate: true,
              aiReplyMarkup: true,
              postIntervalSec: true,
              lastPostAt: true,
            },
          },
          mediaAsset: {
            select: {
              id: true,
              telegramFileId: true,
              status: true,
              originalName: true,
              aiGeneratedCaption: true,
              durationSec: true,
              sourceMeta: true,
              localPath: true,
            },
          },
        },
      }),
    { label: 'dispatch.handleDispatchJob.findTask' },
  );

  if (!task) {
    throw new Error(`未找到分发任务: ${dispatchTaskIdRaw}`);
  }

  if (task.groupKey) {
    const delayTo = new Date(Date.now() + 10 * 1000);
    await withPrismaRetry(
      () =>
        prisma.dispatchTask.update({
          where: { id: dispatchTaskId },
          data: {
            status: TaskStatus.scheduled,
            nextRunAt: delayTo,
          },
        }),
      { label: 'dispatch.handleDispatchJob.groupMasterRedirect' },
    );

    await withPrismaRetry(
      () =>
        prisma.dispatchTaskLog.create({
          data: {
            dispatchTaskId,
            action: 'task_group_master_redirected',
            detail: {
              groupKey: task.groupKey,
              reason: 'grouped_assets_must_use_dispatch_send_group',
              nextRunAt: delayTo.toISOString(),
            },
          },
        }),
      { label: 'dispatch.handleDispatchJob.groupMasterRedirectLog' },
    );

    logger.info('[typeb_group] 单条任务重定向到组任务主路径', {
      dispatchTaskId: dispatchTaskIdRaw,
      channelId: task.channelId.toString(),
      groupKey: task.groupKey,
      nextRunAt: delayTo.toISOString(),
      typeb_group_master_redirect_total: 1,
    });

    return {
      ok: true,
      redirected: true,
      reason: 'group_master_redirected',
      dispatchTaskId: dispatchTaskIdRaw,
      groupKey: task.groupKey,
    };
  }



  await withPrismaRetry(
    () =>
      prisma.dispatchTask.update({
        where: { id: dispatchTaskId },
        data: {
          status: TaskStatus.running,
          startedAt: new Date(),
        },
      }),
    { label: 'dispatch.handleDispatchJob.markRunning' },
  );

  await withPrismaRetry(
    () =>
      prisma.dispatchTaskLog.create({
        data: {
          dispatchTaskId,
          action: 'task_running',
          detail: {
            jobId,
            attemptsMade,
          },
        },
      }),
    { label: 'dispatch.handleDispatchJob.logRunning' },
  );

  try {
    if (!task.mediaAsset.telegramFileId) {
      throw new Error('媒体资源缺少 telegramFileId（中转尚未完成）');
    }

    const mediaSourceMeta =
      task.mediaAsset.sourceMeta && typeof task.mediaAsset.sourceMeta === 'object'
        ? (task.mediaAsset.sourceMeta as Record<string, unknown>)
        : null;
    const originalNameStem = getFileStem(task.mediaAsset.originalName);
    const runtimeHint =
      typeof task.mediaAsset.durationSec === 'number' && task.mediaAsset.durationSec > 0
        ? `视频实测时长约 ${Math.floor(task.mediaAsset.durationSec / 60)} 分 ${task.mediaAsset.durationSec % 60} 秒（请据此填写“单集片长”，不要瞎编）`
        : '未探测到可靠视频时长（“单集片长”请谨慎表述为未知或约略，不要乱填具体分钟数）';

    const collectionMeta = parseCollectionSourceMeta(mediaSourceMeta);
    const isCollectionAsset = collectionMeta.isCollection;
    const collectionName = collectionMeta.collectionName ?? '';
    const episodeNo = collectionMeta.episodeNo;

    const enhancedCollectionVideoName =
      isCollectionAsset && collectionName && episodeNo !== null
        ? `${buildCollectionEpisodeTitle(collectionName, episodeNo)} ${originalNameStem}`.trim()
        : null;

    const aiSearchVideoName = enhancedCollectionVideoName ?? originalNameStem;

    const aiUserPrompt =
      isCollectionAsset && collectionName && episodeNo !== null
        ? [
            '请为这个合集视频生成文案（仅针对本条视频）。',
            `基础视频名：${task.mediaAsset.originalName}`,
            `增强视频名：${aiSearchVideoName}`,
            `合集名：${collectionName}`,
            `集数：第${episodeNo}集`,
            runtimeHint,
            '要求：必须优先依据“增强视频名（合集名+第N集+视频名）”进行搜索与理解，再按系统提示词要求的格式输出；禁止编造。',
          ].join('\n')
        : [
            '请为这个视频生成文案。',
            `视频名：${aiSearchVideoName}`,
            runtimeHint,
            '要求：按系统提示词要求的格式输出；信息不确定时请明确标注未知，不要编造。',
          ].join('\n');

    let finalCaption = task.caption || task.mediaAsset.aiGeneratedCaption;
    if (isAiFailureText(finalCaption)) {
      finalCaption = originalNameStem;
    }

    const aiProfile = await resolveDispatchAiProfile(task.channel.aiModelProfileId);

    if (task.channel.aiSystemPromptTemplate && task.channel.cloneUseAiPromptTemplate && aiProfile) {
      try {
        finalCaption = await generateTextWithAiProfile(
          aiProfile,
          task.channel.aiSystemPromptTemplate,
          aiUserPrompt,
        );

        if (isAiFailureText(finalCaption)) {
          finalCaption = originalNameStem;
        }

        await prisma.dispatchTask.update({
          where: { id: task.id },
          data: { caption: finalCaption },
        });
        await prisma.mediaAsset.update({
          where: { id: task.mediaAsset.id },
          data: { aiGeneratedCaption: finalCaption },
        });
      } catch (aiErr) {
        logError('[q_dispatch] AI 文案生成失败', {
          dispatchTaskId: task.id.toString(),
          error: aiErr,
        });
        finalCaption = finalCaption || originalNameStem;
      }
    } else if (!finalCaption) {
      finalCaption = originalNameStem;
    }

    if (isAiFailureText(finalCaption)) {
      finalCaption = originalNameStem;
    }

    finalCaption = sanitizeTypeBCaptionUnknown(finalCaption || '', originalNameStem || '精彩视频');

    if (isCollectionAsset && collectionName && episodeNo !== null) {
      finalCaption = applyCollectionEpisodeTitle(
        finalCaption || '',
        buildCollectionEpisodeTitle(collectionName, episodeNo),
      );
    }

    if (!task.mediaAsset.aiGeneratedCaption && finalCaption) {
      await prisma.mediaAsset.update({
        where: { id: task.mediaAsset.id },
        data: { aiGeneratedCaption: finalCaption },
      });
    }

    if (aiProfile) {
      try {
        await classifyAndAssignForTypeB({
          mediaAssetId: task.mediaAsset.id,
          originalName: aiSearchVideoName,
          aiCaption: finalCaption || '',
          durationSec: task.mediaAsset.durationSec,
          profile: aiProfile,
        });
      } catch (categoryErr) {
        logError('[q_dispatch] 自动分类失败（不阻塞发送）', {
          dispatchTaskId: task.id.toString(),
          mediaAssetId: task.mediaAsset.id.toString(),
          error: categoryErr,
        });
      }
    }

    try {
      await assignContentTagsForTypeB({
        mediaAssetId: task.mediaAsset.id,
        channelId: task.channel.id,
        originalName: aiSearchVideoName,
        aiCaption: finalCaption || null,
        sourceMeta: task.mediaAsset.sourceMeta,
        profile: aiProfile,
        triggerSource: 'dispatch_typeb',
        enqueueSearchIndex: false,
      });
    } catch (tagErr) {
      logError('[q_dispatch] 成人内容自动标签失败（不阻塞发送）', {
        dispatchTaskId: task.id.toString(),
        mediaAssetId: task.mediaAsset.id.toString(),
        error: tagErr,
      });
    }

    finalCaption = finalCaption || originalNameStem;

    const sourceRelayBotIdRaw = mediaSourceMeta?.relayBotId;
    const sourceRelayBotId =
      typeof sourceRelayBotIdRaw === 'string' && /^\d+$/.test(sourceRelayBotIdRaw)
        ? BigInt(sourceRelayBotIdRaw)
        : null;

    const resolvedBotId = sourceRelayBotId ?? task.botId ?? task.channel.defaultBotId;
    if (!resolvedBotId) {
      throw new Error('分发任务或频道未配置机器人');
    }

    const bot = await withPrismaRetry(
      () =>
        prisma.bot.findFirst({
          where: {
            id: resolvedBotId,
            status: 'active',
          },
          select: { id: true, tokenEncrypted: true },
        }),
      { label: 'dispatch.handleDispatchJob.findActiveBot' },
    );

    if (!bot) {
      throw new Error(
        `未找到可用机器人: dispatchTaskId=${task.id.toString()}, resolvedBotId=${resolvedBotId.toString()}`,
      );
    }

    if (DISPATCH_CHANNEL_INTERVAL_GUARD_ENABLED) {
      const now = new Date();
      const intervalSec = Math.max(0, task.channel.postIntervalSec ?? 0);
      const nextAllowedAt = task.channel.lastPostAt
        ? new Date(task.channel.lastPostAt.getTime() + intervalSec * 1000)
        : now;

      if (nextAllowedAt.getTime() > now.getTime()) {
        await prisma.dispatchTask.update({
          where: { id: dispatchTaskId },
          data: {
            status: TaskStatus.scheduled,
            nextRunAt: nextAllowedAt,
            finishedAt: now,
          },
        });

        await prisma.dispatchTaskLog.create({
          data: {
            dispatchTaskId,
            action: 'task_deferred_by_channel_interval',
            detail: {
              channelId: task.channelId.toString(),
              postIntervalSec: intervalSec,
              lastPostAt: task.channel.lastPostAt?.toISOString() ?? null,
              nextAllowedAt: nextAllowedAt.toISOString(),
            },
          },
        });

        logger.info('[q_dispatch] 任务延后（未到频道发送窗口）', {
          dispatchTaskId: dispatchTaskIdRaw,
          channelId: task.channelId.toString(),
          postIntervalSec: intervalSec,
          lastPostAt: task.channel.lastPostAt?.toISOString() ?? null,
          nextAllowedAt: nextAllowedAt.toISOString(),
        });

        return {
          ok: true,
          skipped: true,
          reason: 'channel_interval_not_due',
          dispatchTaskId: dispatchTaskIdRaw,
        };
      }
    }

    const dispatchMethod = resolveDispatchMethod(mediaSourceMeta, task.mediaAsset.originalName);

    logger.info('[q_dispatch] 最终选用发送方法', {
      dispatchTaskId: dispatchTaskIdRaw,
      channelId: task.channelId.toString(),
      mediaAssetId: task.mediaAsset.id.toString(),
      dispatchMethod,
      relayResolvedMediaType:
        typeof mediaSourceMeta?.relayResolvedMediaType === 'string'
          ? mediaSourceMeta.relayResolvedMediaType
          : null,
      mimeType: typeof mediaSourceMeta?.mimeType === 'string' ? mediaSourceMeta.mimeType : null,
      originalName: task.mediaAsset.originalName,
    });

    // ?? send With Method ?????????????????????
    const sendWithMethod = async (method: 'sendVideo' | 'sendPhoto', parseMode: string | null | undefined) => {
      if (method === 'sendPhoto') {
        return sendPhotoByTelegram({
          botToken: bot.tokenEncrypted,
          chatId: task.channel.tgChatId,
          fileId: task.mediaAsset.telegramFileId,
          caption: finalCaption,
          parseMode,
          replyMarkup: task.replyMarkup ?? task.channel.aiReplyMarkup ?? undefined,
        });
      }

      return sendVideoByTelegram({
        botToken: bot.tokenEncrypted,
        chatId: task.channel.tgChatId,
        fileId: task.mediaAsset.telegramFileId,
        caption: finalCaption,
        parseMode,
        replyMarkup: task.replyMarkup ?? task.channel.aiReplyMarkup ?? undefined,
      });
    };

    let sendResult;
    try {
      sendResult = await sendWithMethod(dispatchMethod, task.parseMode);
    } catch (sendError) {
      const errorObj = sendError as TelegramError;

      if (isPhotoAsVideoError({ code: errorObj.code, message: errorObj.message })) {
        logger.warn('[q_dispatch] 识别到 Photo 被当作 Video，自动降级为 sendPhoto 重试', {
          dispatchTaskId: dispatchTaskIdRaw,
          channelId: task.channelId.toString(),
          originalMethod: dispatchMethod,
          retryMethod: 'sendPhoto',
          errorCode: errorObj.code,
          errorMessage: errorObj.message,
        });

        sendResult = await sendWithMethod('sendPhoto', task.parseMode);
      } else if (
        task.parseMode?.toUpperCase() === 'HTML' &&
        isParseEntitiesError({ code: errorObj.code, message: errorObj.message })
      ) {
        logger.warn('[q_dispatch] HTML 解析失败，回退纯文本重发', {
          dispatchTaskId: dispatchTaskIdRaw,
          channelId: task.channelId.toString(),
          method: dispatchMethod,
          errorCode: errorObj.code,
          errorMessage: errorObj.message,
        });

        sendResult = await sendWithMethod(dispatchMethod, null);
      } else {
        throw sendError;
      }
    }

    await withPrismaRetry(
      () =>
        prisma.$transaction([
          prisma.dispatchTask.update({
            where: { id: dispatchTaskId },
            data: {
              status: TaskStatus.success,
              finishedAt: new Date(),
              botId: bot.id,
              telegramMessageId: BigInt(sendResult.messageId),
              telegramMessageLink: sendResult.messageLink,
              telegramErrorCode: null,
              telegramErrorMessage: null,
            },
          }),
          prisma.channel.update({
            where: { id: task.channelId },
            data: { lastPostAt: new Date() },
          }),
        ]),
      { label: 'dispatch.handleDispatchJob.markSuccessAndUpdateChannel' },
    );

    if (isCollectionAsset) {
      const deletedCount = await deleteCatalogSourceItemFromSingle({
        channelId: task.channelId,
        telegramMessageId: sendResult.messageId,
      });
      catalogSourceWriteMetrics.skippedCollectionTotal += 1;
      logger.info('[catalog_source_item] skip collection single projection', {
        dispatchTaskId: dispatchTaskIdRaw,
        channelId: task.channelId.toString(),
        mediaAssetId: task.mediaAsset.id.toString(),
        telegramMessageId: String(sendResult.messageId),
        collectionName,
        episodeNo,
        deletedCount,
      });
    } else {
      await upsertCatalogSourceItemFromSingle({
        channelId: task.channelId,
        dispatchTaskId,
        telegramMessageId: sendResult.messageId,
        telegramMessageLink: sendResult.messageLink || null,
        caption: finalCaption || null,
        title: extractCatalogShortTitle(finalCaption),
      });
    }

    await prisma.dispatchTaskLog.create({
      data: {
        dispatchTaskId,
        action: 'task_success',
        detail: {
          botId: bot.id.toString(),
          messageId: sendResult.messageId,
          messageLink: sendResult.messageLink,
        },
      },
    });

    await cleanupDispatchScopedDirectoriesAfterSuccess([
      {
        mediaAsset: {
          sourceMeta: task.mediaAsset.sourceMeta,
          localPath: task.mediaAsset.localPath,
        },
      },
    ]);

    // ── 触发搜索索引更新（延迟2秒等AI caption等后续处理完成）──
    try {
      await searchIndexQueue.add('upsert', {
        sourceType: 'dispatch_task',
        sourceId: task.id.toString(),
        mediaAssetId: task.mediaAsset.id.toString(),
        channelId: task.channelId.toString(),
      }, {
        delay: 2000,
        jobId: `search-index-asset-${task.mediaAsset.id}`,
      });
    } catch (indexErr) {
      // 搜索索引失败不阻塞主流程
      logError('[q_dispatch] 搜索索引入队失败（不阻塞）', { error: indexErr });
    }

    return {
      ok: true,
      dispatchTaskId: dispatchTaskIdRaw,
      messageId: sendResult.messageId,
    };

  } catch (error) {
    const nextRetryCount = task.retryCount + 1;
    const now = new Date();

    const errorObj = error as TelegramError;
    const message = errorObj.message || '未知分发错误';
    const code = errorObj.code || 'DISPATCH_ERROR';

    const retryAfterSec = errorObj.retryAfterSec;
    const fallbackBackoffSec = getBackoffSeconds(nextRetryCount);
    const finalBackoffSec = retryAfterSec ?? fallbackBackoffSec;
    const nextRunAt = new Date(Date.now() + finalBackoffSec * 1000);

    const exceeded = nextRetryCount > task.maxRetries;
    const deterministic = isDeterministicDispatchError({ code, message });

    const nextStatus = exceeded || deterministic ? TaskStatus.dead : TaskStatus.failed;

    await withPrismaRetry(
      () =>
        prisma.dispatchTask.update({
          where: { id: dispatchTaskId },
          data: {
            status: nextStatus,
            retryCount: nextRetryCount,
            nextRunAt: nextStatus === TaskStatus.dead ? task.nextRunAt : nextRunAt,
            telegramErrorCode: code,
            telegramErrorMessage: message,
            finishedAt: now,
          },
        }),
      { label: 'dispatch.handleDispatchJob.markFailedOrDead' },
    );

    await withPrismaRetry(
      () =>
        prisma.dispatchTaskLog.create({
          data: {
            dispatchTaskId,
            action: nextStatus === TaskStatus.dead ? 'task_dead' : 'task_failed',
            detail: {
              errorCode: code,
              errorMessage: message,
              retryCount: nextRetryCount,
              nextRunAt: nextStatus === TaskStatus.dead ? null : nextRunAt,
              deterministic,
            },
          },
        }),
      { label: 'dispatch.handleDispatchJob.logFailedOrDead' },
    );

    if (code === 'TG_429' || code === 'TG_403') {
      await withPrismaRetry(
        () =>
          prisma.riskEvent.create({
            data: {
              level: code === 'TG_429' ? 'high' : 'critical',
              eventType:
                code === 'TG_429'
                  ? 'telegram_rate_limit'
                  : 'telegram_permission_denied',
              botId: task.botId ?? task.channel.defaultBotId,
              channelId: task.channelId,
              dispatchTaskId: task.id,
              payload: {
                telegramErrorCode: code,
                telegramErrorMessage: message,
                retryCount: nextRetryCount,
                jobId,
              },
            },
          }),
        { label: 'dispatch.handleDispatchJob.createRiskEvent' },
      );
    }

    throw error;
  }
}

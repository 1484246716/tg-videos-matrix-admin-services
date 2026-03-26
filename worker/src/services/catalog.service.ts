import { TaskStatus, CatalogTaskStatus } from '@prisma/client';
import { CATALOG_CHANNEL_INTERVAL_GUARD_ENABLED } from '../config/env';
import { prisma } from '../infra/prisma';
import { logError } from '../logger';
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
    if (errObj.message?.includes('message is not modified')) {
      return { messageId: existing, isNewMessage: false };
    }
    if (errObj.message?.includes('message to edit not found')) {
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
  detailMessageIds: Record<string, number>;
};

function parseCollectionNavState(rawNavReplyMarkup: unknown): CollectionNavState {
  if (!rawNavReplyMarkup || typeof rawNavReplyMarkup !== 'object' || Array.isArray(rawNavReplyMarkup)) {
    return { indexMessageId: null, detailMessageIds: {} };
  }

  const container = rawNavReplyMarkup as Record<string, unknown>;
  const state =
    container.__collectionNavState && typeof container.__collectionNavState === 'object'
      ? (container.__collectionNavState as Record<string, unknown>)
      : null;

  if (!state) return { indexMessageId: null, detailMessageIds: {} };

  const indexMessageIdRaw = state.indexMessageId;
  const indexMessageId =
    typeof indexMessageIdRaw === 'number' && Number.isInteger(indexMessageIdRaw) && indexMessageIdRaw > 0
      ? indexMessageIdRaw
      : null;

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

  return { indexMessageId, detailMessageIds };
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
    detailMessageIds: state.detailMessageIds,
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

  const videos = orderedDispatchTasks.map((t) => {
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

  const collectionGroups = new Map<string, Array<{ episodeNo: number; title: string; messageUrl: string }>>();
  for (const task of orderedDispatchTasks) {
    if (!task.telegramMessageLink) continue;
    const meta = parseCollectionMeta(task.mediaAsset?.sourceMeta);
    if (!meta) continue;

    const sourceMeta =
      task.mediaAsset?.sourceMeta && typeof task.mediaAsset.sourceMeta === 'object'
        ? (task.mediaAsset.sourceMeta as Record<string, unknown>)
        : {};
    const fallbackTitle = getFileStem(task.mediaAsset?.originalName || '未命名视频');
    const customCatalogTitle =
      typeof sourceMeta.catalogCustomTitle === 'string' ? sourceMeta.catalogCustomTitle.trim() : '';
    const safeCaption = isAiFailureText(task.caption) ? fallbackTitle : (task.caption || '').trim();

    const group = collectionGroups.get(meta.collectionName) ?? [];
    group.push({
      episodeNo: meta.episodeNo,
      title: customCatalogTitle || buildCatalogShortTitle(safeCaption, fallbackTitle),
      messageUrl: task.telegramMessageLink,
    });
    collectionGroups.set(meta.collectionName, group);
  }

  const navPageSize = Math.max(1, Math.min(100, (channel as any).navPageSize ?? 10));
  const navPagingEnabled = typeof (channel as any).navPagingEnabled === 'boolean'
    ? (channel as any).navPagingEnabled
    : false;
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
  let nextCollectionNavState: CollectionNavState | null = null;
  if (collectionGroups.size > 0) {
    const names = [...collectionGroups.keys()].sort((a, b) => a.localeCompare(b, 'zh-Hans-CN'));
    const collectionIndexButtons: Array<Array<{ text: string; url: string }>> = [];

    const existingNavState = parseCollectionNavState(channel.navReplyMarkup);
    const detailMessageIds: Record<string, number> = {};

    collectionSections.push('📚 合集导航\n点击按钮跳转到对应合集详情');

    for (let idx = 0; idx < names.length; idx += 1) {
      const name = names[idx];
      const episodes = (collectionGroups.get(name) ?? []).sort((a, b) => a.episodeNo - b.episodeNo);
      const lines = episodes.map((ep) => `第${ep.episodeNo}集 ${ep.title}\n${ep.messageUrl}`);

      const detailPublishResult = await publishCatalogMessage({
        botToken,
        chatId: channel.tgChatId,
        text: [`📺 ${name}`, ...lines].join('\n'),
        existingMessageId: existingNavState.detailMessageIds[name] ?? null,
      });

      detailMessageIds[name] = detailPublishResult.messageId;
      const detailLink = toTelegramMessageLink(channel.tgChatId, detailPublishResult.messageId);
      if (detailLink) {
        collectionIndexButtons.push([{ text: `${idx + 1}) ${name}`, url: detailLink }]);
      }
    }

    let indexMessageId: number | null = null;
    if (collectionIndexButtons.length > 0) {
      const indexPublishResult = await publishCatalogMessage({
        botToken,
        chatId: channel.tgChatId,
        text: '📚 合集索引\n请选择合集：',
        existingMessageId: existingNavState.indexMessageId,
        replyMarkup: { inline_keyboard: collectionIndexButtons },
      });
      indexMessageId = indexPublishResult.messageId;
    }

    const staleCollectionNames = Object.keys(existingNavState.detailMessageIds).filter(
      (name) => !Object.prototype.hasOwnProperty.call(detailMessageIds, name),
    );
    for (const staleName of staleCollectionNames) {
      const staleMessageId = existingNavState.detailMessageIds[staleName];
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

    nextCollectionNavState = {
      indexMessageId,
      detailMessageIds,
    };
  }

  const content = [pageContents.join('\n\n'), ...collectionSections].filter(Boolean).join('\n\n');
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
        text: pageContents[0],
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
        const publishResult = await publishCatalogMessage({
          botToken,
          chatId: channel.tgChatId,
          text: pageContents[pageIndex],
          existingMessageId: storedPageMessageIds[pageIndex] ?? null,
        });
        publishedPageMessageIds.push(publishResult.messageId);
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

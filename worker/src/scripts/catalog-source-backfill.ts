import '../config/env';
import { prisma } from '../infra/prisma';
import {
  TYPEC_CATALOG_SOURCE_BACKFILL_BATCH_SIZE,
  TYPEC_CATALOG_SOURCE_BACKFILL_SLEEP_MS,
} from '../config/env';
import '../config/env';
import { prisma } from '../infra/prisma';
import {
  TYPEC_CATALOG_SOURCE_BACKFILL_BATCH_SIZE,
  TYPEC_CATALOG_SOURCE_BACKFILL_SLEEP_MS,
} from '../config/env';
import { logger } from '../logger';

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getCheckpointFromArg() {
  const arg = process.argv.find((item) => item.startsWith('--from-id='));
  if (!arg) return BigInt(0);
  const value = arg.slice('--from-id='.length).trim();
  if (!/^\d+$/.test(value)) return BigInt(0);
  return BigInt(value);
}

function getChannelIdFromArg() {
  const arg = process.argv.find((item) => item.startsWith('--channel-id='));
  if (!arg) return null;
  const value = arg.slice('--channel-id='.length).trim();
  if (!/^\d+$/.test(value)) return null;
  return BigInt(value);
}

function normalizeCaptionText(raw?: string | null) {
  if (!raw) return '';
  return raw.replace(/^\uFEFF/, '').trim();
}

function truncateCatalogTitle(text: string, maxChars = 15) {
  const chars = Array.from(text);
  if (chars.length <= maxChars) return text;
  return `${chars.slice(0, maxChars).join('')}...`;
}

function normalizeTitleFallback(raw: string, fallback: string) {
  const candidate = raw
    .replace(/[#]/g, '') // 不再删除《》
    .replace(/未知/g, '')
    .trim();
  if (candidate) return candidate;

  const cleanFallback = fallback.replace(/[#]/g, '').trim();
  return cleanFallback || '精彩视频';
}

function extractCatalogShortTitle(raw?: string | null) {
  const caption = normalizeCaptionText(raw);
  if (!caption) return null;

  const lines = caption.split('\n').map((l) => l.trim()).filter(Boolean);
  
  // 1. 优先寻找包含 "片名" 的行
  const titleLineIndex = lines.findIndex((l) => l.includes('片名') || l.startsWith('📺'));
  
  if (titleLineIndex !== -1) {
    let combinedTitle = lines[titleLineIndex];
    
    // 如果下一行包含 "主演"，先合并进来
    const nextLine = lines[titleLineIndex + 1];
    if (nextLine && (nextLine.includes('主演') || nextLine.includes('👤'))) {
      combinedTitle += ' ' + nextLine;
    }

    // 增强判断：剔除无用的主演信息
    // 匹配 "主演：未知"、"主演：不适用" 以及前面的表情符号
    combinedTitle = combinedTitle.replace(/[\s👤👥]*主演\s*[：:]\s*(?:未知|不适用)/g, '').trim();

    return truncateCatalogTitle(combinedTitle, 60);
  }

  // 2. 回退逻辑：取第一行内容
  const firstLine = lines[0] || '';
  return truncateCatalogTitle(firstLine, 40);
}

function buildGroupMessageLink(tgChatId: string | null, messageId: bigint | null): string | null {
  if (!tgChatId || !messageId) return null;
  const prefix = tgChatId.startsWith('-100') ? tgChatId.slice(4) : tgChatId.replace(/^-/, '');
  return `https://t.me/c/${prefix}/${messageId.toString()}`;
}

function isCollectionSourceMeta(sourceMeta: unknown) {
  if (!sourceMeta || typeof sourceMeta !== 'object') return false;
  return (sourceMeta as Record<string, unknown>).isCollection === true;
}

async function deleteSingleProjection(channelId: bigint, messageId: number) {
  const result = await (prisma as any).catalogSourceItem.deleteMany({
    where: {
      channelId,
      telegramMessageId: BigInt(messageId),
    },
  });
  return Number(result?.count || 0);
}

async function deleteGroupProjection(channelId: bigint, groupKey: string, firstMessageId: number | null) {
  const result = await (prisma as any).catalogSourceItem.deleteMany({
    where: {
      channelId,
      OR: [
        { groupKey },
        ...(firstMessageId ? [{ telegramMessageId: BigInt(firstMessageId) }] : []),
      ],
    },
  });
  return Number(result?.count || 0);
}

async function upsertSingle(row: {
  id: bigint;
  channelId: bigint;
  caption: string | null;
  telegramMessageId: bigint | null;
  telegramMessageLink: string | null;
  finishedAt: Date | null;
  mediaAsset: { sourceMeta: unknown; originalName?: string; aiGeneratedCaption?: string | null } | null;
}) {
  const messageId = row.telegramMessageId ? Number(row.telegramMessageId) : null;
  if (!messageId) return;

  if (isCollectionSourceMeta(row.mediaAsset?.sourceMeta)) {
    return {
      action: 'skipped_collection' as const,
      deletedCount: await deleteSingleProjection(row.channelId, messageId),
    };
  }

  const finalCaption = row.caption || row.mediaAsset?.aiGeneratedCaption || row.mediaAsset?.originalName || '';
  const title = extractCatalogShortTitle(finalCaption) || normalizeTitleFallback(finalCaption, '精彩视频');

  await (prisma as any).catalogSourceItem.upsert({
    where: {
      channelId_telegramMessageId: {
        channelId: row.channelId,
        telegramMessageId: BigInt(messageId),
      },
    },
    update: {
      telegramMessageLink: row.telegramMessageLink,
      sourceType: 'single',
      title,
      caption: finalCaption,
      sourceDispatchTaskId: row.id,
      publishedAt: row.finishedAt ?? new Date(),
    },
    create: {
      channelId: row.channelId,
      telegramMessageId: BigInt(messageId),
      telegramMessageLink: row.telegramMessageLink,
      sourceType: 'single',
      title,
      caption: finalCaption,
      sourceDispatchTaskId: row.id,
      publishedAt: row.finishedAt ?? new Date(),
    },
  });

  return { action: 'upserted' as const, deletedCount: 0 };
}

async function upsertGroup(
  groupKey: string,
  seedTask: {
    id: bigint;
    channelId: bigint;
    caption: string | null;
    finishedAt: Date | null;
    telegramMessageId: bigint | null;
    telegramMessageLink: string | null;
    mediaAsset: { sourceMeta: unknown; originalName?: string; aiGeneratedCaption?: string | null } | null;
  },
  groupTask: {
    telegramFirstMessageId: bigint | null;
  } | null,
  channelChatId: string | null,
) {
  const firstMessageId = groupTask?.telegramFirstMessageId
    ? Number(groupTask.telegramFirstMessageId)
    : seedTask.telegramMessageId
      ? Number(seedTask.telegramMessageId)
      : null;
  if (isCollectionSourceMeta(seedTask.mediaAsset?.sourceMeta)) {
    return {
      action: 'skipped_collection' as const,
      deletedCount: await deleteGroupProjection(seedTask.channelId, groupKey, firstMessageId),
    };
  }
  if (!firstMessageId) return { action: 'skipped_missing_message' as const, deletedCount: 0 };

  const telegramMessageLink = buildGroupMessageLink(channelChatId, BigInt(firstMessageId)) || seedTask.telegramMessageLink;
  const finalCaption = seedTask.caption || seedTask.mediaAsset?.aiGeneratedCaption || seedTask.mediaAsset?.originalName || '';
  const title = extractCatalogShortTitle(finalCaption) || normalizeTitleFallback(finalCaption, '精彩视频');

  await (prisma as any).catalogSourceItem.upsert({
    where: {
      channelId_telegramMessageId: {
        channelId: seedTask.channelId,
        telegramMessageId: BigInt(firstMessageId),
      },
    },
    update: {
      telegramMessageLink,
      sourceType: 'group',
      groupKey,
      title,
      caption: finalCaption,
      sourceDispatchTaskId: seedTask.id,
      publishedAt: seedTask.finishedAt ?? new Date(),
    },
    create: {
      channelId: seedTask.channelId,
      telegramMessageId: BigInt(firstMessageId),
      telegramMessageLink,
      sourceType: 'group',
      groupKey,
      title,
      caption: finalCaption,
      sourceDispatchTaskId: seedTask.id,
      publishedAt: seedTask.finishedAt ?? new Date(),
    },
  });

  return { action: 'upserted' as const, deletedCount: 0 };
}

async function main() {
  const batchSize = TYPEC_CATALOG_SOURCE_BACKFILL_BATCH_SIZE;
  const sleepMs = TYPEC_CATALOG_SOURCE_BACKFILL_SLEEP_MS;
  let cursor = getCheckpointFromArg();
  const filterChannelId = getChannelIdFromArg();

  logger.info('[catalog_source_backfill] started', {
    batchSize,
    sleepMs,
    fromId: cursor.toString(),
    filterChannelId: filterChannelId?.toString() ?? null,
  });

  let totalProcessed = 0;
  let totalUpserted = 0;
  let totalSkippedCollections = 0;
  let totalDeletedCollections = 0;

  while (true) {
    const rows = await prisma.dispatchTask.findMany({
      where: {
        id: { gt: cursor },
        status: 'success',
        telegramMessageId: { not: null },
        ...(filterChannelId ? { channelId: filterChannelId } : {}),
      },
      orderBy: { id: 'asc' },
      take: batchSize,
      select: {
        id: true,
        channelId: true,
        groupKey: true,
        caption: true,
        telegramMessageId: true,
        telegramMessageLink: true,
        finishedAt: true,
        mediaAsset: {
          select: {
            sourceMeta: true,
            originalName: true,
            aiGeneratedCaption: true,
          },
        },
      },
    });

    if (rows.length === 0) {
      break;
    }

    const singles = rows.filter((r) => !r.groupKey);
    const groups = rows.filter((r) => r.groupKey);

    for (const row of singles) {
      const result = await upsertSingle(row);
      if (result.action === 'upserted') {
        totalUpserted += 1;
      } else if (result.action === 'skipped_collection') {
        totalSkippedCollections += 1;
        totalDeletedCollections += result.deletedCount;
      }
    }

    if (groups.length > 0) {
      const groupKeySet = new Map<string, { channelId: bigint; groupKey: string }>();
      for (const row of groups) {
        const key = `${row.channelId}:${row.groupKey}`;
        if (!groupKeySet.has(key)) {
          groupKeySet.set(key, { channelId: row.channelId, groupKey: row.groupKey! });
        }
      }

      const groupKeys = Array.from(groupKeySet.values());

      const groupTasks = await (prisma as any).dispatchGroupTask.findMany({
        where: {
          OR: groupKeys.map((g) => ({
            channelId: g.channelId,
            groupKey: g.groupKey,
          })),
        },
        select: {
          channelId: true,
          groupKey: true,
          telegramFirstMessageId: true,
        },
      });

      const groupTaskMap = new Map<string, { telegramFirstMessageId: bigint | null }>();
      for (const gt of groupTasks) {
        groupTaskMap.set(`${gt.channelId}:${gt.groupKey}`, {
          telegramFirstMessageId: gt.telegramFirstMessageId,
        });
      }

      const channelIds = [...new Set(groups.map((r) => r.channelId))];
      const channels = await prisma.channel.findMany({
        where: { id: { in: channelIds } },
        select: { id: true, tgChatId: true },
      });
      const channelChatIdMap = new Map<bigint, string | null>();
      for (const ch of channels) {
        channelChatIdMap.set(ch.id, ch.tgChatId);
      }

      const seedByGroupKey = new Map<string, typeof groups[0]>();
      for (const row of groups) {
        const key = `${row.channelId}:${row.groupKey}`;
        if (!seedByGroupKey.has(key)) {
          seedByGroupKey.set(key, row);
        }
      }

      for (const [key, seedTask] of seedByGroupKey) {
        const groupTaskInfo = groupTaskMap.get(key) ?? null;
        const chatId = channelChatIdMap.get(seedTask.channelId) ?? null;

        const result = await upsertGroup(seedTask.groupKey!, seedTask, groupTaskInfo, chatId);
        if (result.action === 'upserted') {
          totalUpserted += 1;
        } else if (result.action === 'skipped_collection') {
          totalSkippedCollections += 1;
          totalDeletedCollections += result.deletedCount;
        }
      }
    }

    totalProcessed += rows.length;
    cursor = rows[rows.length - 1].id;

    logger.info('[catalog_source_backfill] batch done', {
      batchRows: rows.length,
      singlesInBatch: singles.length,
      groupsInBatch: groups.length,
      totalProcessed,
      totalUpserted,
      totalSkippedCollections,
      totalDeletedCollections,
      checkpointDispatchTaskId: cursor.toString(),
    });

    if (sleepMs > 0) {
      await sleep(sleepMs);
    }
  }

  logger.info('[catalog_source_backfill] completed', {
    totalProcessed,
    totalUpserted,
    totalSkippedCollections,
    totalDeletedCollections,
    finalCheckpointDispatchTaskId: cursor.toString(),
  });
}

main()
  .catch((error) => {
    logger.error('[catalog_source_backfill] failed', {
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : null,
    });
    process.exitCode = 1;
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

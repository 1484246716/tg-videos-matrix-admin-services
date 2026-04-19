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

function parseCollectionMeta(sourceMeta: unknown) {
  if (!sourceMeta || typeof sourceMeta !== 'object') {
    return { isCollection: false, collectionName: null as string | null, episodeNo: null as number | null };
  }

  const meta = sourceMeta as Record<string, unknown>;
  const isCollection = meta.isCollection === true;
  if (!isCollection) {
    return { isCollection: false, collectionName: null as string | null, episodeNo: null as number | null };
  }

  const collectionName = typeof meta.collectionName === 'string' ? meta.collectionName.trim() : '';
  const episodeNo =
    typeof meta.episodeNo === 'number'
      ? meta.episodeNo
      : typeof meta.episodeNo === 'string' && /^\d+$/.test(meta.episodeNo)
        ? Number(meta.episodeNo)
        : null;

  return {
    isCollection: true,
    collectionName: collectionName || null,
    episodeNo,
  };
}

async function main() {
  const batchSize = TYPEC_CATALOG_SOURCE_BACKFILL_BATCH_SIZE;
  const sleepMs = TYPEC_CATALOG_SOURCE_BACKFILL_SLEEP_MS;
  let cursor = getCheckpointFromArg();

  logger.info('[catalog_source_backfill] started', {
    batchSize,
    sleepMs,
    fromId: cursor.toString(),
  });

  let totalProcessed = 0;
  let totalUpserted = 0;

  while (true) {
    const rows = await prisma.dispatchTask.findMany({
      where: {
        id: { gt: cursor },
        status: 'success',
        telegramMessageId: { not: null },
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
          },
        },
      },
    });

    if (rows.length === 0) {
      break;
    }

    for (const row of rows) {
      const messageId = row.telegramMessageId ? Number(row.telegramMessageId) : null;
      if (!messageId) {
        continue;
      }

      const meta = row.mediaAsset?.sourceMeta;
      const collection = parseCollectionMeta(meta);
      const sourceType = row.groupKey ? 'group' : 'single';
      const title = (row.caption || '').trim() || null;

      await (prisma as any).catalogSourceItem.upsert({
        where: {
          channelId_telegramMessageId: {
            channelId: row.channelId,
            telegramMessageId: BigInt(messageId),
          },
        },
        update: {
          telegramMessageLink: row.telegramMessageLink,
          sourceType,
          groupKey: row.groupKey,
          title,
          caption: row.caption,
          isCollection: collection.isCollection,
          collectionName: collection.collectionName,
          episodeNo: collection.episodeNo,
          sourceDispatchTaskId: row.id,
          publishedAt: row.finishedAt ?? new Date(),
        },
        create: {
          channelId: row.channelId,
          telegramMessageId: BigInt(messageId),
          telegramMessageLink: row.telegramMessageLink,
          sourceType,
          groupKey: row.groupKey,
          title,
          caption: row.caption,
          isCollection: collection.isCollection,
          collectionName: collection.collectionName,
          episodeNo: collection.episodeNo,
          sourceDispatchTaskId: row.id,
          publishedAt: row.finishedAt ?? new Date(),
        },
      });

      totalUpserted += 1;
    }

    totalProcessed += rows.length;
    cursor = rows[rows.length - 1].id;

    logger.info('[catalog_source_backfill] batch done', {
      batchRows: rows.length,
      totalProcessed,
      totalUpserted,
      checkpointDispatchTaskId: cursor.toString(),
    });

    if (sleepMs > 0) {
      await sleep(sleepMs);
    }
  }

  logger.info('[catalog_source_backfill] completed', {
    totalProcessed,
    totalUpserted,
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

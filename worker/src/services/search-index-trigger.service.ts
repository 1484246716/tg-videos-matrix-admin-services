/**
 * ????????????????????????????????????
 * ?????bootstrap ???? -> enqueueChangedCollectionEpisodes -> q_search_index -> search-index.worker?
 */

import { prisma } from '../infra/prisma';
import { searchIndexQueue } from '../infra/redis';
import { logger, logError } from '../logger';

let lastEpisodeCursorAt: Date = new Date(0);

// ?????????????????????????????
export async function enqueueChangedCollectionEpisodes(batchSize = 200) {
  try {
    const rows = await prisma.collectionEpisode.findMany({
      where: {
        updatedAt: {
          gt: lastEpisodeCursorAt,
        },
      },
      orderBy: [{ updatedAt: 'asc' }, { id: 'asc' }],
      take: Math.max(1, Math.min(batchSize, 1000)),
      select: {
        id: true,
        updatedAt: true,
      },
    });

    if (rows.length === 0) return { scanned: 0, enqueued: 0 };

    let enqueued = 0;

    for (const row of rows) {
      await searchIndexQueue.add(
        'upsert',
        {
          sourceType: 'collection_episode',
          sourceId: row.id.toString(),
        },
        {
          jobId: `search-index-collection-episode-${row.id.toString()}`,
          removeOnComplete: true,
          removeOnFail: 200,
        },
      );
      enqueued += 1;
    }

    lastEpisodeCursorAt = rows[rows.length - 1].updatedAt;

    logger.info('[search-index-trigger] collection_episode 变更已入队', {
      scanned: rows.length,
      enqueued,
      cursorAt: lastEpisodeCursorAt.toISOString(),
    });

    return { scanned: rows.length, enqueued };
  } catch (error) {
    logError('[search-index-trigger] collection_episode 变更扫描失败', error);
    return { scanned: 0, enqueued: 0 };
  }
}

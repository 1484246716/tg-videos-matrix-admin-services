import { prisma } from '../infra/prisma';
import { logger } from '../logger';

function parseArg(name: string, defaultValue: string) {
  const hit = process.argv.find((item) => item.startsWith(`--${name}=`));
  if (!hit) return defaultValue;
  return hit.slice(name.length + 3).trim() || defaultValue;
}

function toInt(value: string, fallback: number, min: number, max: number) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

async function main() {
  const channelIdRaw = parseArg('channelId', '');
  if (!/^\d+$/.test(channelIdRaw)) {
    throw new Error('请传入 --channelId=数字');
  }

  const sampleSize = toInt(parseArg('sampleSize', '200'), 200, 10, 5000);

  const channelId = BigInt(channelIdRaw);

  const dispatchRows = await prisma.dispatchTask.findMany({
    where: {
      channelId,
      status: 'success',
      telegramMessageId: { not: null },
    },
    orderBy: { id: 'desc' },
    take: sampleSize,
    select: {
      id: true,
      telegramMessageId: true,
      telegramMessageLink: true,
      groupKey: true,
      finishedAt: true,
    },
  });

  const sourceRows = await (prisma as any).catalogSourceItem.findMany({
    where: { channelId },
    orderBy: { sourceDispatchTaskId: 'desc' },
    take: sampleSize * 2,
    select: {
      sourceDispatchTaskId: true,
      telegramMessageId: true,
      telegramMessageLink: true,
      sourceType: true,
      groupKey: true,
      publishedAt: true,
    },
  });

  const sourceByMessageId = new Map<string, any>();
  for (const row of sourceRows) {
    sourceByMessageId.set(String(row.telegramMessageId), row);
  }

  let matched = 0;
  let missing = 0;
  let linkMismatch = 0;
  let typeMismatch = 0;

  const mismatchSamples: Array<Record<string, unknown>> = [];

  for (const row of dispatchRows) {
    const key = String(row.telegramMessageId);
    const mapped = sourceByMessageId.get(key);
    if (!mapped) {
      missing += 1;
      if (mismatchSamples.length < 20) {
        mismatchSamples.push({
          type: 'missing',
          dispatchTaskId: row.id.toString(),
          telegramMessageId: key,
          expectedSourceType: row.groupKey ? 'group' : 'single',
        });
      }
      continue;
    }

    matched += 1;

    const expectedType = row.groupKey ? 'group' : 'single';
    if (mapped.sourceType !== expectedType) {
      typeMismatch += 1;
      if (mismatchSamples.length < 20) {
        mismatchSamples.push({
          type: 'source_type_mismatch',
          dispatchTaskId: row.id.toString(),
          telegramMessageId: key,
          expected: expectedType,
          actual: mapped.sourceType,
        });
      }
    }

    if ((row.telegramMessageLink || '') !== (mapped.telegramMessageLink || '')) {
      linkMismatch += 1;
      if (mismatchSamples.length < 20) {
        mismatchSamples.push({
          type: 'link_mismatch',
          dispatchTaskId: row.id.toString(),
          telegramMessageId: key,
          dispatchLink: row.telegramMessageLink,
          sourceLink: mapped.telegramMessageLink,
        });
      }
    }
  }

  logger.info('[catalog_source_reconcile] sample summary', {
    channelId: channelIdRaw,
    sampleSize,
    dispatchSampleRows: dispatchRows.length,
    sourceSampleRows: sourceRows.length,
    matched,
    missing,
    linkMismatch,
    typeMismatch,
    mismatchSamples,
  });
}

main()
  .catch((error) => {
    logger.error('[catalog_source_reconcile] failed', {
      error: error instanceof Error ? error.message : String(error),
      stack: error instanceof Error ? error.stack : null,
    });
    process.exitCode = 1;
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

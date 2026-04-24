/**
 * catalog_source_item 抽样对账脚本。
 * 对比 dispatch 成功样本与 catalog_source_item 映射的一致性。
 */

import '../config/env';
import { prisma } from '../infra/prisma';
import { logger } from '../logger';

// 解析命令行参数（--name=value）。
function parseArg(name: string, defaultValue: string) {
  const hit = process.argv.find((item) => item.startsWith(`--${name}=`));
  if (!hit) return defaultValue;
  const eqIndex = hit.indexOf('=');
  if (eqIndex < 0) return defaultValue;
  return hit.slice(eqIndex + 1).trim() || defaultValue;
}

// 安全转换为整数并限制在给定范围内。
function toInt(value: string, fallback: number, min: number, max: number) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

// 判断 sourceMeta 是否标记为合集来源。
function isCollectionSourceMeta(sourceMeta: unknown) {
  if (!sourceMeta || typeof sourceMeta !== 'object') return false;
  return (sourceMeta as Record<string, unknown>).isCollection === true;
}

// 主流程：抽样读取、对账统计并输出结果。
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
        mediaAsset: {
          select: {
            sourceMeta: true,
          },
        },
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

  const sourceDispatchIds = [...new Set(
    sourceRows
      .map((row: any) => row.sourceDispatchTaskId)
      .filter((id: unknown) => typeof id === 'bigint'),
  )];
  const sourceDispatchMetaRows = sourceDispatchIds.length > 0
    ? await prisma.dispatchTask.findMany({
        where: { id: { in: sourceDispatchIds as bigint[] } },
        select: {
          id: true,
          mediaAsset: {
            select: {
              sourceMeta: true,
            },
          },
        },
      })
    : [];
  const sourceDispatchMetaMap = new Map(
    sourceDispatchMetaRows.map((row) => [row.id.toString(), row.mediaAsset?.sourceMeta]),
  );

  let collectionRowsInCatalogSource = 0;
  const eligibleSourceRows = sourceRows.filter((row: any) => {
    const sourceMeta =
      row.sourceDispatchTaskId !== null && row.sourceDispatchTaskId !== undefined
        ? sourceDispatchMetaMap.get(String(row.sourceDispatchTaskId))
        : undefined;
    const isCollection = isCollectionSourceMeta(sourceMeta);
    if (isCollection) {
      collectionRowsInCatalogSource += 1;
      return false;
    }
    return true;
  });

  const sourceByMessageId = new Map<string, any>();
  for (const row of eligibleSourceRows) {
    sourceByMessageId.set(String(row.telegramMessageId), row);
  }

  const sourceByGroupKey = new Map<string, any>();
  for (const row of eligibleSourceRows) {
    if (row.groupKey) {
      sourceByGroupKey.set(row.groupKey, row);
    }
  }

  let matched = 0;
  let missing = 0;
  let linkMismatch = 0;
  let typeMismatch = 0;
  let groupSkipped = 0;
  let collectionDispatchSkipped = 0;

  const mismatchSamples: Array<Record<string, unknown>> = [];

  const seenGroupKeys = new Set<string>();

  for (const row of dispatchRows) {
    if (isCollectionSourceMeta(row.mediaAsset?.sourceMeta)) {
      collectionDispatchSkipped += 1;
      continue;
    }

    const expectedType = row.groupKey ? 'group' : 'single';

    if (row.groupKey) {
      if (seenGroupKeys.has(row.groupKey)) {
        groupSkipped += 1;
        continue;
      }
      seenGroupKeys.add(row.groupKey);

      const mapped = sourceByGroupKey.get(row.groupKey);
      if (!mapped) {
        missing += 1;
        if (mismatchSamples.length < 20) {
          mismatchSamples.push({
            type: 'missing_group',
            dispatchTaskId: row.id.toString(),
            groupKey: row.groupKey,
            expectedSourceType: 'group',
          });
        }
        continue;
      }

      matched += 1;

      if (mapped.sourceType !== 'group') {
        typeMismatch += 1;
        if (mismatchSamples.length < 20) {
          mismatchSamples.push({
            type: 'source_type_mismatch',
            dispatchTaskId: row.id.toString(),
            groupKey: row.groupKey,
            expected: 'group',
            actual: mapped.sourceType,
          });
        }
      }
    } else {
      const key = String(row.telegramMessageId);
      const mapped = sourceByMessageId.get(key);
      if (!mapped) {
        missing += 1;
        if (mismatchSamples.length < 20) {
          mismatchSamples.push({
            type: 'missing_single',
            dispatchTaskId: row.id.toString(),
            telegramMessageId: key,
            expectedSourceType: 'single',
          });
        }
        continue;
      }

      matched += 1;

      if (mapped.sourceType !== 'single') {
        typeMismatch += 1;
        if (mismatchSamples.length < 20) {
          mismatchSamples.push({
            type: 'source_type_mismatch',
            dispatchTaskId: row.id.toString(),
            telegramMessageId: key,
            expected: 'single',
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
  }

  logger.info('[catalog_source_reconcile] sample summary', {
    channelId: channelIdRaw,
    sampleSize,
    dispatchSampleRows: dispatchRows.length,
    sourceSampleRows: sourceRows.length,
    eligibleSourceSampleRows: eligibleSourceRows.length,
    matched,
    missing,
    linkMismatch,
    typeMismatch,
    groupSkipped,
    collectionDispatchSkipped,
    collectionRowsInCatalogSource,
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

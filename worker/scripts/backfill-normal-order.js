const path = require('path');
const dotenv = require('dotenv');
const { PrismaClient } = require('@prisma/client');

dotenv.config({ path: path.resolve(__dirname, '../../.env') });
dotenv.config();

const prisma = new PrismaClient();

function asObject(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  return value;
}

function parseIntegerOrderNo(value) {
  if (typeof value === 'number' && Number.isFinite(value) && value !== 0) {
    return Math.trunc(value);
  }
  if (typeof value === 'string' && /^-?\d+$/.test(value.trim())) {
    const parsed = Number(value.trim());
    if (Number.isFinite(parsed) && parsed !== 0) {
      return Math.trunc(parsed);
    }
  }
  return null;
}

function buildNormalOrderGroup(channelId) {
  return `normal:${channelId.toString()}`;
}

function buildNormalOrderMeta(channelId, orderNo) {
  return {
    orderType: 'normal',
    orderGroup: buildNormalOrderGroup(channelId),
    orderNo: Math.floor(orderNo),
    orderParseFailed: false,
  };
}

function resolveOrderMeta(channelId, sourceMeta) {
  const meta = asObject(sourceMeta);
  const collectionName =
    typeof meta.collectionName === 'string' && meta.collectionName.trim()
      ? meta.collectionName.trim()
      : null;
  const episodeNo = (() => {
    const parsed = parseIntegerOrderNo(meta.episodeNo);
    return parsed !== null && parsed > 0 ? parsed : null;
  })();
  const isCollection =
    meta.isCollection === true || meta.orderType === 'collection' || Boolean(collectionName);
  const orderType = isCollection ? 'collection' : 'normal';
  const orderGroup =
    typeof meta.orderGroup === 'string' && meta.orderGroup.trim()
      ? meta.orderGroup.trim()
      : buildNormalOrderGroup(channelId);
  const orderNo = parseIntegerOrderNo(meta.orderNo) ?? (isCollection ? episodeNo : null);
  const orderParseFailed =
    typeof meta.orderParseFailed === 'boolean'
      ? meta.orderParseFailed
      : isCollection
        ? meta.episodeParseFailed === true || orderNo === null
        : false;

  return {
    orderType,
    orderGroup,
    orderNo,
    orderParseFailed,
    isCollection,
  };
}

function parseChannelIds(raw) {
  if (!raw || !raw.trim()) {
    return null;
  }
  const values = raw
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean);
  if (values.length === 0) {
    return null;
  }
  return values.map((item) => BigInt(item));
}

function parsePositiveInt(raw, fallback) {
  if (raw === undefined || raw === null || raw === '') {
    return fallback;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.floor(parsed);
}

function parseBooleanFlag(raw, fallback) {
  if (raw === undefined) {
    return fallback;
  }
  const normalized = String(raw).trim().toLowerCase();
  if (normalized === 'true') return true;
  if (normalized === 'false') return false;
  return fallback;
}

async function loadChannels(channelIds, maxChannels) {
  const channels = await prisma.channel.findMany({
    where: channelIds ? { id: { in: channelIds } } : undefined,
    select: { id: true, name: true },
    orderBy: { id: 'asc' },
    take: maxChannels ?? undefined,
  });
  return channels;
}

async function buildChannelPlan(channel) {
  const assets = await prisma.mediaAsset.findMany({
    where: { channelId: channel.id },
    select: {
      id: true,
      createdAt: true,
      sourceMeta: true,
      status: true,
    },
    orderBy: [{ createdAt: 'asc' }, { id: 'asc' }],
  });

  const normalAssets = assets.filter((asset) => resolveOrderMeta(channel.id, asset.sourceMeta).orderType === 'normal');
  const currentOrderNos = normalAssets
    .map((asset) => resolveOrderMeta(channel.id, asset.sourceMeta))
    .filter((meta) => meta.orderNo !== null)
    .map((meta) => meta.orderNo || 0);

  let nextOrderNo = currentOrderNos.length > 0 ? Math.max(...currentOrderNos) + 1 : 1;
  const updates = [];
  let missingOrderNoCount = 0;

  for (const asset of normalAssets) {
    const sourceMeta = asObject(asset.sourceMeta);
    const resolved = resolveOrderMeta(channel.id, asset.sourceMeta);
    if (resolved.orderNo === null) {
      missingOrderNoCount += 1;
    }

    const targetOrderNo = resolved.orderNo ?? nextOrderNo;
    const targetMeta = buildNormalOrderMeta(channel.id, targetOrderNo);
    const shouldUpdate =
      resolved.orderNo === null ||
      sourceMeta.orderType !== targetMeta.orderType ||
      sourceMeta.orderGroup !== targetMeta.orderGroup ||
      sourceMeta.orderNo !== targetMeta.orderNo ||
      sourceMeta.orderParseFailed !== targetMeta.orderParseFailed;

    if (!shouldUpdate) {
      continue;
    }

    updates.push({
      id: asset.id,
      targetMeta,
      nextSourceMeta: {
        ...sourceMeta,
        ...targetMeta,
        orderBackfilled: true,
        orderBackfilledAt: new Date().toISOString(),
        orderBackfillBasis: 'createdAt_id',
      },
      previousOrderNo: resolved.orderNo,
      status: asset.status,
      createdAt: asset.createdAt.toISOString(),
    });

    if (resolved.orderNo === null) {
      nextOrderNo += 1;
    }
  }

  return {
    channelId: channel.id,
    channelName: channel.name,
    totalAssets: assets.length,
    normalAssets: normalAssets.length,
    missingOrderNoCount,
    updateCount: updates.length,
    updates,
  };
}

async function applyPlan(plan, batchSize) {
  let applied = 0;
  for (let offset = 0; offset < plan.updates.length; offset += batchSize) {
    const batch = plan.updates.slice(offset, offset + batchSize);
    await prisma.$transaction(
      batch.map((item) =>
        prisma.mediaAsset.update({
          where: { id: item.id },
          data: {
            sourceMeta: item.nextSourceMeta,
          },
        }),
      ),
    );
    applied += batch.length;
  }
  return applied;
}

async function main() {
  const dryRun = parseBooleanFlag(process.env.DRY_RUN, true);
  const channelIds = parseChannelIds(process.env.CHANNEL_IDS);
  const maxChannels = parsePositiveInt(process.env.MAX_CHANNELS, null);
  const batchSize = parsePositiveInt(process.env.BATCH_SIZE, 100);

  console.log(
    JSON.stringify(
      {
        step: 'start',
        dryRun,
        channelIds: channelIds?.map((item) => item.toString()) ?? null,
        maxChannels,
        batchSize,
      },
      null,
      2,
    ),
  );

  const channels = await loadChannels(channelIds, maxChannels);
  const summary = {
    channelCount: channels.length,
    touchedChannelCount: 0,
    totalAssets: 0,
    totalNormalAssets: 0,
    totalMissingOrderNo: 0,
    totalPlannedUpdates: 0,
    totalAppliedUpdates: 0,
  };

  for (const channel of channels) {
    const plan = await buildChannelPlan(channel);
    summary.totalAssets += plan.totalAssets;
    summary.totalNormalAssets += plan.normalAssets;
    summary.totalMissingOrderNo += plan.missingOrderNoCount;
    summary.totalPlannedUpdates += plan.updateCount;

    if (plan.updateCount > 0) {
      summary.touchedChannelCount += 1;
    }

    console.log(
      JSON.stringify(
        {
          step: 'channel_plan',
          channelId: plan.channelId.toString(),
          channelName: plan.channelName,
          totalAssets: plan.totalAssets,
          normalAssets: plan.normalAssets,
          missingOrderNoCount: plan.missingOrderNoCount,
          updateCount: plan.updateCount,
          sampleUpdates: plan.updates.slice(0, 5).map((item) => ({
            mediaAssetId: item.id.toString(),
            previousOrderNo: item.previousOrderNo,
            targetOrderNo: item.targetMeta.orderNo,
            createdAt: item.createdAt,
            status: item.status,
          })),
        },
        null,
        2,
      ),
    );

    if (!dryRun && plan.updateCount > 0) {
      summary.totalAppliedUpdates += await applyPlan(plan, batchSize);
    }
  }

  console.log(
    JSON.stringify(
      {
        step: 'done',
        dryRun,
        ...summary,
      },
      null,
      2,
    ),
  );
}

main()
  .catch((error) => {
    console.error(
      JSON.stringify(
        {
          step: 'failed',
          message: error instanceof Error ? error.message : String(error),
          stack: error instanceof Error ? error.stack : null,
        },
        null,
        2,
      ),
    );
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

import '../config/env';
import { prisma } from '../infra/prisma';
import { refreshCollectionSnapshotIncremental } from '../services/collection-snapshot.service';

function parseArg(name: string, defaultValue: string) {
  const hit = process.argv.find((item) => item.startsWith(`--${name}=`));
  if (!hit) return defaultValue;
  const eqIndex = hit.indexOf('=');
  if (eqIndex < 0) return defaultValue;
  return hit.slice(eqIndex + 1).trim() || defaultValue;
}

async function main() {
  const channelId = BigInt(parseArg('channelId', '10'));
  const collectionName = parseArg('collection', '黄宏11');
  const navPageSize = Math.max(1, Math.min(100, Number(parseArg('navPageSize', '1')) || 1));

  const collection = await prisma.collection.findFirst({
    where: {
      channelId,
      OR: [
        { name: collectionName },
        { nameNormalized: collectionName.normalize('NFKC').replace(/\s+/g, ' ').trim() },
      ],
    },
    select: { id: true, name: true, nameNormalized: true, navPageSize: true },
  });

  if (!collection) {
    throw new Error(`Collection not found: channelId=${channelId.toString()} collection=${collectionName}`);
  }

  const updatedCollection = await prisma.collection.update({
    where: { id: collection.id },
    data: { navPageSize },
    select: { id: true, name: true, nameNormalized: true, navPageSize: true },
  });

  const snapshotResult = await refreshCollectionSnapshotIncremental();

  const snapshotRows = await prisma.collectionEpisodeSnapshot.findMany({
    where: {
      channelId,
      collectionNameNormalized: updatedCollection.nameNormalized,
    },
    orderBy: { episodeNo: 'asc' },
    select: {
      episodeNo: true,
      telegramMessageId: true,
      title: true,
    },
  });

  const snapshotHead = await prisma.collectionSnapshot.findUnique({
    where: {
      channelId_collectionNameNormalized: {
        channelId,
        collectionNameNormalized: updatedCollection.nameNormalized,
      },
    },
    select: {
      episodeCount: true,
      minEpisodeNo: true,
      maxEpisodeNo: true,
      lastRebuildAt: true,
    },
  });

  console.log(JSON.stringify({
    ok: true,
    collection: updatedCollection,
    snapshotResult,
    snapshotHead,
    snapshotRows,
  }, (_key, value) => (typeof value === 'bigint' ? value.toString() : value), 2));
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

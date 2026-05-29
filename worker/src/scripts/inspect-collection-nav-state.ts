import '../config/env';
import { prisma } from '../infra/prisma';

function parseArg(name: string, defaultValue: string) {
  const hit = process.argv.find((item) => item.startsWith(`--${name}=`));
  if (!hit) return defaultValue;
  const eqIndex = hit.indexOf('=');
  if (eqIndex < 0) return defaultValue;
  return hit.slice(eqIndex + 1).trim() || defaultValue;
}

async function main() {
  const channelId = BigInt(parseArg('channelId', '10'));
  const channel = await prisma.channel.findUnique({
    where: { id: channelId },
    select: { id: true, name: true, navReplyMarkup: true, lastNavUpdateAt: true },
  });

  if (!channel) throw new Error(`Channel not found: ${channelId.toString()}`);

  const nav = channel.navReplyMarkup as any;
  const collectionNavState = nav?.__collectionNavState ?? null;
  const hashState = nav?.__catalogHashState ?? null;

  const detailPageMessageIds = collectionNavState?.detailPageMessageIds ?? {};
  const detailMessageIds = collectionNavState?.detailMessageIds ?? {};
  const detailCounts = Object.fromEntries(
    Object.entries(detailPageMessageIds).map(([name, ids]) => [name, Array.isArray(ids) ? ids.length : 0]),
  );

  console.log(JSON.stringify({
    channel: {
      id: channel.id,
      name: channel.name,
      lastNavUpdateAt: channel.lastNavUpdateAt,
    },
    indexMessageId: collectionNavState?.indexMessageId ?? null,
    indexPageMessageIds: collectionNavState?.indexPageMessageIds ?? [],
    detailMessageIds,
    detailPageMessageIds,
    detailCounts,
    hashCollections: Object.keys(hashState?.collection_detail ?? {}),
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

import '../config/env';
import { handleCatalogJob } from '../services/catalog.service';
import { prisma } from '../infra/prisma';

function parseArg(name: string, defaultValue: string) {
  const hit = process.argv.find((item) => item.startsWith(`--${name}=`));
  if (!hit) return defaultValue;
  const eqIndex = hit.indexOf('=');
  if (eqIndex < 0) return defaultValue;
  return hit.slice(eqIndex + 1).trim() || defaultValue;
}

async function main() {
  const channelId = parseArg('channelId', '10');
  const result = await handleCatalogJob(channelId, {
    triggerType: 'manual_repair',
    selfHealOnly: false,
    forceRepublish: true,
  });

  console.log(JSON.stringify(result, (_key, value) => (typeof value === 'bigint' ? value.toString() : value), 2));
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

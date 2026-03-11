import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

async function main() {
  const asset = await prisma.mediaAsset.findFirst({
    where: {
      originalName: {
        contains: '小崔说事',
      },
    },
    include: {
      dispatchTasks: true,
      channel: true
    }
  });

  console.log(JSON.stringify(asset, (key, value) => {
    return typeof value === 'bigint' ? value.toString() : value;
  }, 2));
}

main()
  .catch(console.error)
  .finally(() => prisma.$disconnect());

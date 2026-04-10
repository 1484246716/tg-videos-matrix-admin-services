const { PrismaClient } = require('@prisma/client');

const prisma = new PrismaClient();

async function main() {
  const db = await prisma.$queryRawUnsafe(
    'select current_database() as db, current_schema() as schema',
  );

  const tables = await prisma.$queryRawUnsafe(
    "SELECT to_regclass('public.clone_crawl_tasks')::text AS clone_crawl_tasks, to_regclass('public.clone_crawl_items')::text AS clone_crawl_items, to_regclass('public.clone_crawl_runs')::text AS clone_crawl_runs, to_regclass('public.clone_crawl_task_channels')::text AS clone_crawl_task_channels",
  );

  const migrations = await prisma.$queryRawUnsafe(
    'SELECT migration_name, finished_at FROM "_prisma_migrations" ORDER BY finished_at DESC LIMIT 8',
  );

  console.log(
    JSON.stringify(
      {
        db: db[0],
        tables: tables[0],
        migrations,
      },
      null,
      2,
    ),
  );
}

main()
  .catch((err) => {
    console.error(err);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

BEGIN;

ALTER TABLE "clone_crawl_tasks"
  ADD COLUMN IF NOT EXISTS "interval_seconds" INTEGER;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_enum e
    JOIN pg_type t ON t.oid = e.enumtypid
    WHERE t.typname = 'CloneScheduleType' AND e.enumlabel = 'interval'
  ) THEN
    ALTER TYPE "CloneScheduleType" ADD VALUE 'interval';
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'clone_crawl_tasks_interval_seconds_check'
  ) THEN
    ALTER TABLE "clone_crawl_tasks"
      ADD CONSTRAINT "clone_crawl_tasks_interval_seconds_check"
      CHECK (
        "interval_seconds" IS NULL
        OR ("interval_seconds" >= 60 AND "interval_seconds" <= 86400)
      );
  END IF;
END $$;

COMMIT;

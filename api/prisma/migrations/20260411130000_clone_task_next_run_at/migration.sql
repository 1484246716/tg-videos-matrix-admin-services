-- AlterTable
ALTER TABLE "clone_crawl_tasks"
ADD COLUMN "daily_run_time" VARCHAR(5),
ADD COLUMN "next_run_at" TIMESTAMPTZ(6),
ADD COLUMN "last_run_at" TIMESTAMPTZ(6);

-- CreateIndex
CREATE INDEX "clone_crawl_tasks_status_next_run_at_idx"
ON "clone_crawl_tasks"("status", "next_run_at");

-- Normalize daily default run time
UPDATE "clone_crawl_tasks"
SET "daily_run_time" = '00:00'
WHERE "schedule_type" = 'daily'
  AND ("daily_run_time" IS NULL OR "daily_run_time" = '');

-- Backfill running tasks with schedule-aware next_run_at
UPDATE "clone_crawl_tasks"
SET "next_run_at" = NULL
WHERE "status" = 'running'
  AND "schedule_type" = 'once'
  AND EXISTS (
    SELECT 1 FROM "clone_crawl_runs" r
    WHERE r."task_id" = "clone_crawl_tasks"."id"
      AND r."status" IN ('success', 'failed', 'partial_success')
  );

UPDATE "clone_crawl_tasks"
SET "next_run_at" = NOW()
WHERE "status" = 'running'
  AND "next_run_at" IS NULL
  AND "schedule_type" IN ('once', 'hourly', 'daily');
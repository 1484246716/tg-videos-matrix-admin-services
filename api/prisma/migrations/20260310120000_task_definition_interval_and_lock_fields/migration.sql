-- Add unified scheduler fields for TaskDefinition (TypeA/TypeB/TypeC)
ALTER TABLE "task_definitions"
  ADD COLUMN IF NOT EXISTS "run_interval_sec" INTEGER NOT NULL DEFAULT 1800,
  ADD COLUMN IF NOT EXISTS "next_run_at" TIMESTAMPTZ(6),
  ADD COLUMN IF NOT EXISTS "last_started_at" TIMESTAMPTZ(6);

-- Backfill next_run_at for enabled rows to be picked up by scheduler
UPDATE "task_definitions"
SET "next_run_at" = NOW()
WHERE "is_enabled" = TRUE
  AND "next_run_at" IS NULL;

-- Helpful index for due task lookup
CREATE INDEX IF NOT EXISTS "task_definitions_is_enabled_next_run_at_idx"
  ON "task_definitions" ("is_enabled", "next_run_at");

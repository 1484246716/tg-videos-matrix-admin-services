-- Enforce grouped dispatch upload gate fields on dispatch_group_tasks

ALTER TABLE "dispatch_group_tasks"
  ADD COLUMN IF NOT EXISTS "actual_uploaded_count" INTEGER,
  ADD COLUMN IF NOT EXISTS "sealed_at" TIMESTAMPTZ(6),
  ADD COLUMN IF NOT EXISTS "seal_reason" VARCHAR(64);

-- Backfill historical rows with safe defaults
UPDATE "dispatch_group_tasks"
SET
  "actual_uploaded_count" = COALESCE("actual_uploaded_count", 0)
WHERE
  "actual_uploaded_count" IS NULL;

ALTER TABLE "dispatch_group_tasks"
  ALTER COLUMN "actual_uploaded_count" SET DEFAULT 0;

ALTER TABLE "dispatch_group_tasks"
  ALTER COLUMN "actual_uploaded_count" SET NOT NULL;

CREATE INDEX IF NOT EXISTS "dispatch_group_tasks_sealed_at_idx"
  ON "dispatch_group_tasks" ("sealed_at");

CREATE INDEX IF NOT EXISTS "dispatch_group_tasks_seal_reason_idx"
  ON "dispatch_group_tasks" ("seal_reason");

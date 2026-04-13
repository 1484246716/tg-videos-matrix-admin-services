-- TypeB grouped send v2: dispatch_group_tasks readiness and idempotency fields

ALTER TABLE "dispatch_group_tasks"
  ADD COLUMN IF NOT EXISTS "ready_deadline_at" TIMESTAMPTZ(6),
  ADD COLUMN IF NOT EXISTS "expected_media_count" INTEGER,
  ADD COLUMN IF NOT EXISTS "actual_ready_count" INTEGER,
  ADD COLUMN IF NOT EXISTS "caption_source" VARCHAR(64),
  ADD COLUMN IF NOT EXISTS "content_fingerprint" VARCHAR(128),
  ADD COLUMN IF NOT EXISTS "dispatch_version" INTEGER;

-- Backfill for historical rows
UPDATE "dispatch_group_tasks"
SET
  "expected_media_count" = COALESCE("expected_media_count", 0),
  "actual_ready_count" = COALESCE("actual_ready_count", 0),
  "dispatch_version" = COALESCE("dispatch_version", 1),
  "ready_deadline_at" = COALESCE("ready_deadline_at", "schedule_slot" + INTERVAL '90 seconds')
WHERE
  "expected_media_count" IS NULL
  OR "actual_ready_count" IS NULL
  OR "dispatch_version" IS NULL
  OR "ready_deadline_at" IS NULL;

ALTER TABLE "dispatch_group_tasks"
  ALTER COLUMN "expected_media_count" SET DEFAULT 0,
  ALTER COLUMN "actual_ready_count" SET DEFAULT 0,
  ALTER COLUMN "dispatch_version" SET DEFAULT 1;

ALTER TABLE "dispatch_group_tasks"
  ALTER COLUMN "expected_media_count" SET NOT NULL,
  ALTER COLUMN "actual_ready_count" SET NOT NULL,
  ALTER COLUMN "dispatch_version" SET NOT NULL;

CREATE INDEX IF NOT EXISTS "dispatch_group_tasks_ready_deadline_at_idx"
  ON "dispatch_group_tasks" ("ready_deadline_at");

CREATE INDEX IF NOT EXISTS "dispatch_group_tasks_status_group_key_idx"
  ON "dispatch_group_tasks" ("status", "group_key");

CREATE INDEX IF NOT EXISTS "dispatch_group_tasks_content_fingerprint_idx"
  ON "dispatch_group_tasks" ("content_fingerprint");

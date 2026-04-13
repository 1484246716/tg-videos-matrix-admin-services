-- TypeB grouped sendMediaGroup: group-level dispatch task table
CREATE TABLE IF NOT EXISTS "dispatch_group_tasks" (
  "id" BIGSERIAL PRIMARY KEY,
  "channel_id" BIGINT NOT NULL,
  "group_key" VARCHAR(128) NOT NULL,
  "schedule_slot" TIMESTAMPTZ(6) NOT NULL,
  "status" "TaskStatus" NOT NULL DEFAULT 'pending',
  "retry_count" INTEGER NOT NULL DEFAULT 0,
  "max_retries" INTEGER NOT NULL DEFAULT 6,
  "next_run_at" TIMESTAMPTZ(6) NOT NULL,
  "telegram_media_group_id" VARCHAR(128),
  "telegram_first_message_id" BIGINT,
  "telegram_error_code" VARCHAR(64),
  "telegram_error_message" TEXT,
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT NOW(),
  "updated_at" TIMESTAMPTZ(6) NOT NULL DEFAULT NOW(),
  CONSTRAINT "dispatch_group_tasks_channel_id_fkey"
    FOREIGN KEY ("channel_id") REFERENCES "channels"("id") ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS "dispatch_group_tasks_channel_slot_group_key_key"
  ON "dispatch_group_tasks" ("channel_id", "schedule_slot", "group_key");

CREATE INDEX IF NOT EXISTS "dispatch_group_tasks_status_next_run_at_idx"
  ON "dispatch_group_tasks" ("status", "next_run_at");

CREATE INDEX IF NOT EXISTS "dispatch_group_tasks_channel_id_group_key_idx"
  ON "dispatch_group_tasks" ("channel_id", "group_key");

-- dispatch_tasks group-level idempotency unique constraint (if missing)
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'dispatch_tasks_channel_id_schedule_slot_group_key_key'
  ) THEN
    -- constraint already exists, skip
    NULL;
  ELSIF EXISTS (
    SELECT 1 FROM pg_class
    WHERE relname = 'dispatch_tasks_channel_id_schedule_slot_group_key_key'
      AND relkind = 'i'
  ) THEN
    -- unique index already exists but constraint may not; avoid duplicate-name failure
    NULL;
  ELSE
    ALTER TABLE "dispatch_tasks"
      ADD CONSTRAINT "dispatch_tasks_channel_id_schedule_slot_group_key_key"
      UNIQUE ("channel_id", "schedule_slot", "group_key");
  END IF;
END $$;

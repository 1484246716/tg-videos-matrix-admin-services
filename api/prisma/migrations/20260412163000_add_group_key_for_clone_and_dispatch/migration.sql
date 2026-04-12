-- Add grouped fields to clone_crawl_items
ALTER TABLE "clone_crawl_items"
  ADD COLUMN IF NOT EXISTS "grouped_id" VARCHAR(128),
  ADD COLUMN IF NOT EXISTS "group_key" VARCHAR(128);

CREATE INDEX IF NOT EXISTS "clone_crawl_items_task_id_group_key_idx"
  ON "clone_crawl_items"("task_id", "group_key");

-- Add group_key to dispatch_tasks for TypeB group-level idempotency
ALTER TABLE "dispatch_tasks"
  ADD COLUMN IF NOT EXISTS "group_key" VARCHAR(128);

CREATE UNIQUE INDEX IF NOT EXISTS "dispatch_tasks_channel_id_schedule_slot_group_key_key"
  ON "dispatch_tasks"("channel_id", "schedule_slot", "group_key");

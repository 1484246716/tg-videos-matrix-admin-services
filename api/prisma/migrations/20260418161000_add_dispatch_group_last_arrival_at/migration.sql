-- Add last_arrival_at for quiet-period based sealing

ALTER TABLE "dispatch_group_tasks"
  ADD COLUMN IF NOT EXISTS "last_arrival_at" TIMESTAMPTZ(6);

CREATE INDEX IF NOT EXISTS "dispatch_group_tasks_last_arrival_at_idx"
  ON "dispatch_group_tasks" ("last_arrival_at");

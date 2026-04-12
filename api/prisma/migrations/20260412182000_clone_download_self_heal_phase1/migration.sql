ALTER TABLE "clone_crawl_items"
  ADD COLUMN IF NOT EXISTS "download_lease_until" TIMESTAMPTZ(6),
  ADD COLUMN IF NOT EXISTS "download_heartbeat_at" TIMESTAMPTZ(6),
  ADD COLUMN IF NOT EXISTS "download_worker_job_id" VARCHAR(128),
  ADD COLUMN IF NOT EXISTS "download_attempt" INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS "download_recover_count" INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS "clone_crawl_items_download_lease_until_idx"
  ON "clone_crawl_items"("download_lease_until");

CREATE INDEX IF NOT EXISTS "clone_crawl_items_download_worker_job_id_idx"
  ON "clone_crawl_items"("download_worker_job_id");

-- Clone 下载进度字段（手动 migration）
ALTER TABLE "clone_crawl_items"
  ADD COLUMN IF NOT EXISTS "download_progress_pct" INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS "downloaded_bytes" BIGINT,
  ADD COLUMN IF NOT EXISTS "download_speed_mbps" DECIMAL(10,2);

-- 约束保护：进度百分比范围 0~100
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'clone_crawl_items_download_progress_pct_check'
  ) THEN
    ALTER TABLE "clone_crawl_items"
      ADD CONSTRAINT "clone_crawl_items_download_progress_pct_check"
      CHECK ("download_progress_pct" >= 0 AND "download_progress_pct" <= 100);
  END IF;
END $$;

BEGIN;

CREATE TABLE IF NOT EXISTS "media_asset_tagging_runs" (
  "id" BIGSERIAL NOT NULL,
  "media_asset_id" BIGINT NOT NULL,
  "channel_id" BIGINT NOT NULL,
  "trigger_source" VARCHAR(32) NOT NULL,
  "candidate_count" INTEGER NOT NULL DEFAULT 0,
  "selected_count" INTEGER NOT NULL DEFAULT 0,
  "confidence" DECIMAL(5, 4),
  "status" VARCHAR(16) NOT NULL DEFAULT 'success',
  "reason" TEXT,
  "payload" JSONB,
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "media_asset_tagging_runs_pkey" PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "media_asset_tagging_runs_media_asset_id_created_at_idx"
  ON "media_asset_tagging_runs"("media_asset_id", "created_at" DESC);

CREATE INDEX IF NOT EXISTS "media_asset_tagging_runs_channel_id_created_at_idx"
  ON "media_asset_tagging_runs"("channel_id", "created_at" DESC);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'media_asset_tagging_runs_media_asset_id_fkey'
  ) THEN
    ALTER TABLE "media_asset_tagging_runs"
      ADD CONSTRAINT "media_asset_tagging_runs_media_asset_id_fkey"
      FOREIGN KEY ("media_asset_id") REFERENCES "media_assets"("id") ON DELETE CASCADE ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'media_asset_tagging_runs_channel_id_fkey'
  ) THEN
    ALTER TABLE "media_asset_tagging_runs"
      ADD CONSTRAINT "media_asset_tagging_runs_channel_id_fkey"
      FOREIGN KEY ("channel_id") REFERENCES "channels"("id") ON DELETE CASCADE ON UPDATE CASCADE;
  END IF;
END $$;

COMMIT;

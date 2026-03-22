-- Add persistent ingest timing fields for media lifecycle duration display
ALTER TABLE "media_assets"
  ADD COLUMN IF NOT EXISTS "ingest_started_at" TIMESTAMPTZ(6),
  ADD COLUMN IF NOT EXISTS "ingest_finished_at" TIMESTAMPTZ(6),
  ADD COLUMN IF NOT EXISTS "ingest_duration_sec" INTEGER;

-- Backfill started/finished timestamps from historical created/updated values when possible
UPDATE "media_assets"
SET
  "ingest_started_at" = COALESCE("ingest_started_at", "created_at"),
  "ingest_finished_at" = COALESCE(
    "ingest_finished_at",
    CASE
      WHEN "status" IN ('relay_uploaded', 'failed') THEN "updated_at"
      ELSE NULL
    END
  )
WHERE "ingest_started_at" IS NULL
   OR ("ingest_finished_at" IS NULL AND "status" IN ('relay_uploaded', 'failed'));

-- Backfill duration seconds if both endpoints exist and duration is non-negative
UPDATE "media_assets"
SET "ingest_duration_sec" = GREATEST(
  FLOOR(EXTRACT(EPOCH FROM ("ingest_finished_at" - "ingest_started_at")))::INTEGER,
  0
)
WHERE "ingest_started_at" IS NOT NULL
  AND "ingest_finished_at" IS NOT NULL
  AND "ingest_duration_sec" IS NULL;

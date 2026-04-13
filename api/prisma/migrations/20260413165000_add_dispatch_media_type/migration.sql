-- CreateEnum
CREATE TYPE "DispatchMediaType" AS ENUM ('photo', 'video');

-- AlterTable
ALTER TABLE "media_assets"
ADD COLUMN "dispatch_media_type" "DispatchMediaType";

-- Backfill from existing source_meta relayResolvedMediaType when possible
UPDATE "media_assets"
SET "dispatch_media_type" = CASE
  WHEN lower(coalesce("source_meta"->>'relayResolvedMediaType', '')) IN ('photo', 'image') THEN 'photo'::"DispatchMediaType"
  WHEN lower(coalesce("source_meta"->>'relayResolvedMediaType', '')) = 'video' THEN 'video'::"DispatchMediaType"
  ELSE NULL
END
WHERE "dispatch_media_type" IS NULL;

-- Optional performance index for dispatch preflight checks
CREATE INDEX IF NOT EXISTS "idx_media_assets_dispatch_media_type" ON "media_assets" ("dispatch_media_type");

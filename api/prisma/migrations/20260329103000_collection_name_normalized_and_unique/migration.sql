-- 1) Add normalized name column
ALTER TABLE "collections"
ADD COLUMN "name_normalized" VARCHAR(128);

-- 2) Backfill normalized name from existing name (NFKC-like behavior should be handled in app;
--    DB fallback here uses trim + collapse whitespace)
UPDATE "collections"
SET "name_normalized" = regexp_replace(trim("name"), '\\s+', ' ', 'g')
WHERE "name_normalized" IS NULL;

-- 3) Ensure not null after backfill
ALTER TABLE "collections"
ALTER COLUMN "name_normalized" SET NOT NULL;

-- 4) Add unique index for channel + normalized name to prevent rename/create duplicates
CREATE UNIQUE INDEX "collections_channel_id_name_normalized_key"
ON "collections" ("channel_id", "name_normalized");

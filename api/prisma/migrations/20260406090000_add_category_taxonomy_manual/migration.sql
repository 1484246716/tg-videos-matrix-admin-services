-- 仅包含“分类体系”新增变更，避免误删历史索引/字段

BEGIN;

CREATE TABLE IF NOT EXISTS "category_level1" (
    "id" BIGSERIAL NOT NULL,
    "name" VARCHAR(64) NOT NULL,
    "slug" VARCHAR(64) NOT NULL,
    "sort" INTEGER NOT NULL DEFAULT 0,
    "status" VARCHAR(16) NOT NULL DEFAULT 'active',
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,
    CONSTRAINT "category_level1_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "category_level2" (
    "id" BIGSERIAL NOT NULL,
    "level1_id" BIGINT NOT NULL,
    "name" VARCHAR(64) NOT NULL,
    "slug" VARCHAR(64) NOT NULL,
    "sort" INTEGER NOT NULL DEFAULT 0,
    "status" VARCHAR(16) NOT NULL DEFAULT 'active',
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,
    CONSTRAINT "category_level2_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "media_asset_categories" (
    "id" BIGSERIAL NOT NULL,
    "media_asset_id" BIGINT NOT NULL,
    "level2_id" BIGINT NOT NULL,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "media_asset_categories_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "search_document_categories" (
    "id" BIGSERIAL NOT NULL,
    "search_document_id" BIGINT NOT NULL,
    "level2_id" BIGINT NOT NULL,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "search_document_categories_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "category_level1_name_key" ON "category_level1"("name");
CREATE UNIQUE INDEX IF NOT EXISTS "category_level1_slug_key" ON "category_level1"("slug");
CREATE UNIQUE INDEX IF NOT EXISTS "category_level2_slug_key" ON "category_level2"("slug");
CREATE UNIQUE INDEX IF NOT EXISTS "category_level2_level1_id_name_key" ON "category_level2"("level1_id", "name");
CREATE INDEX IF NOT EXISTS "category_level2_level1_id_status_sort_idx" ON "category_level2"("level1_id", "status", "sort");
CREATE UNIQUE INDEX IF NOT EXISTS "media_asset_categories_media_asset_id_level2_id_key" ON "media_asset_categories"("media_asset_id", "level2_id");
CREATE INDEX IF NOT EXISTS "media_asset_categories_level2_id_media_asset_id_idx" ON "media_asset_categories"("level2_id", "media_asset_id");
CREATE UNIQUE INDEX IF NOT EXISTS "search_document_categories_search_document_id_level2_id_key" ON "search_document_categories"("search_document_id", "level2_id");
CREATE INDEX IF NOT EXISTS "search_document_categories_level2_id_search_document_id_idx" ON "search_document_categories"("level2_id", "search_document_id");

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'category_level2_level1_id_fkey'
  ) THEN
    ALTER TABLE "category_level2"
      ADD CONSTRAINT "category_level2_level1_id_fkey"
      FOREIGN KEY ("level1_id") REFERENCES "category_level1"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'media_asset_categories_media_asset_id_fkey'
  ) THEN
    ALTER TABLE "media_asset_categories"
      ADD CONSTRAINT "media_asset_categories_media_asset_id_fkey"
      FOREIGN KEY ("media_asset_id") REFERENCES "media_assets"("id") ON DELETE CASCADE ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'media_asset_categories_level2_id_fkey'
  ) THEN
    ALTER TABLE "media_asset_categories"
      ADD CONSTRAINT "media_asset_categories_level2_id_fkey"
      FOREIGN KEY ("level2_id") REFERENCES "category_level2"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'search_document_categories_search_document_id_fkey'
  ) THEN
    ALTER TABLE "search_document_categories"
      ADD CONSTRAINT "search_document_categories_search_document_id_fkey"
      FOREIGN KEY ("search_document_id") REFERENCES "search_documents"("id") ON DELETE CASCADE ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'search_document_categories_level2_id_fkey'
  ) THEN
    ALTER TABLE "search_document_categories"
      ADD CONSTRAINT "search_document_categories_level2_id_fkey"
      FOREIGN KEY ("level2_id") REFERENCES "category_level2"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
  END IF;
END $$;

COMMIT;

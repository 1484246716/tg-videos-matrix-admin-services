BEGIN;

CREATE TABLE IF NOT EXISTS "content_tags" (
  "id" BIGSERIAL NOT NULL,
  "name" VARCHAR(64) NOT NULL,
  "slug" VARCHAR(64) NOT NULL,
  "sort" INTEGER NOT NULL DEFAULT 0,
  "status" VARCHAR(16) NOT NULL DEFAULT 'active',
  "scope" VARCHAR(16) NOT NULL DEFAULT 'adult_18',
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "content_tags_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "media_asset_tags" (
  "id" BIGSERIAL NOT NULL,
  "media_asset_id" BIGINT NOT NULL,
  "tag_id" BIGINT NOT NULL,
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "media_asset_tags_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "collection_categories" (
  "id" BIGSERIAL NOT NULL,
  "collection_id" BIGINT NOT NULL,
  "level2_id" BIGINT NOT NULL,
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "collection_categories_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "collection_tags" (
  "id" BIGSERIAL NOT NULL,
  "collection_id" BIGINT NOT NULL,
  "tag_id" BIGINT NOT NULL,
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "collection_tags_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "search_document_tags" (
  "id" BIGSERIAL NOT NULL,
  "search_document_id" BIGINT NOT NULL,
  "tag_id" BIGINT NOT NULL,
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "search_document_tags_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "channel_default_categories" (
  "id" BIGSERIAL NOT NULL,
  "channel_id" BIGINT NOT NULL,
  "level2_id" BIGINT NOT NULL,
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "channel_default_categories_pkey" PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS "channel_default_tags" (
  "id" BIGSERIAL NOT NULL,
  "channel_id" BIGINT NOT NULL,
  "tag_id" BIGINT NOT NULL,
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "channel_default_tags_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "content_tags_name_key" ON "content_tags"("name");
CREATE UNIQUE INDEX IF NOT EXISTS "content_tags_slug_key" ON "content_tags"("slug");
CREATE INDEX IF NOT EXISTS "content_tags_status_scope_sort_idx" ON "content_tags"("status", "scope", "sort");

CREATE UNIQUE INDEX IF NOT EXISTS "media_asset_tags_media_asset_id_tag_id_key"
  ON "media_asset_tags"("media_asset_id", "tag_id");
CREATE INDEX IF NOT EXISTS "media_asset_tags_tag_id_media_asset_id_idx"
  ON "media_asset_tags"("tag_id", "media_asset_id");

CREATE UNIQUE INDEX IF NOT EXISTS "collection_categories_collection_id_level2_id_key"
  ON "collection_categories"("collection_id", "level2_id");
CREATE INDEX IF NOT EXISTS "collection_categories_level2_id_collection_id_idx"
  ON "collection_categories"("level2_id", "collection_id");

CREATE UNIQUE INDEX IF NOT EXISTS "collection_tags_collection_id_tag_id_key"
  ON "collection_tags"("collection_id", "tag_id");
CREATE INDEX IF NOT EXISTS "collection_tags_tag_id_collection_id_idx"
  ON "collection_tags"("tag_id", "collection_id");

CREATE UNIQUE INDEX IF NOT EXISTS "search_document_tags_search_document_id_tag_id_key"
  ON "search_document_tags"("search_document_id", "tag_id");
CREATE INDEX IF NOT EXISTS "search_document_tags_tag_id_search_document_id_idx"
  ON "search_document_tags"("tag_id", "search_document_id");

CREATE UNIQUE INDEX IF NOT EXISTS "channel_default_categories_channel_id_level2_id_key"
  ON "channel_default_categories"("channel_id", "level2_id");
CREATE INDEX IF NOT EXISTS "channel_default_categories_level2_id_channel_id_idx"
  ON "channel_default_categories"("level2_id", "channel_id");

CREATE UNIQUE INDEX IF NOT EXISTS "channel_default_tags_channel_id_tag_id_key"
  ON "channel_default_tags"("channel_id", "tag_id");
CREATE INDEX IF NOT EXISTS "channel_default_tags_tag_id_channel_id_idx"
  ON "channel_default_tags"("tag_id", "channel_id");

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'media_asset_tags_media_asset_id_fkey'
  ) THEN
    ALTER TABLE "media_asset_tags"
      ADD CONSTRAINT "media_asset_tags_media_asset_id_fkey"
      FOREIGN KEY ("media_asset_id") REFERENCES "media_assets"("id") ON DELETE CASCADE ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'media_asset_tags_tag_id_fkey'
  ) THEN
    ALTER TABLE "media_asset_tags"
      ADD CONSTRAINT "media_asset_tags_tag_id_fkey"
      FOREIGN KEY ("tag_id") REFERENCES "content_tags"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'collection_categories_collection_id_fkey'
  ) THEN
    ALTER TABLE "collection_categories"
      ADD CONSTRAINT "collection_categories_collection_id_fkey"
      FOREIGN KEY ("collection_id") REFERENCES "collections"("id") ON DELETE CASCADE ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'collection_categories_level2_id_fkey'
  ) THEN
    ALTER TABLE "collection_categories"
      ADD CONSTRAINT "collection_categories_level2_id_fkey"
      FOREIGN KEY ("level2_id") REFERENCES "category_level2"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'collection_tags_collection_id_fkey'
  ) THEN
    ALTER TABLE "collection_tags"
      ADD CONSTRAINT "collection_tags_collection_id_fkey"
      FOREIGN KEY ("collection_id") REFERENCES "collections"("id") ON DELETE CASCADE ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'collection_tags_tag_id_fkey'
  ) THEN
    ALTER TABLE "collection_tags"
      ADD CONSTRAINT "collection_tags_tag_id_fkey"
      FOREIGN KEY ("tag_id") REFERENCES "content_tags"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'search_document_tags_search_document_id_fkey'
  ) THEN
    ALTER TABLE "search_document_tags"
      ADD CONSTRAINT "search_document_tags_search_document_id_fkey"
      FOREIGN KEY ("search_document_id") REFERENCES "search_documents"("id") ON DELETE CASCADE ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'search_document_tags_tag_id_fkey'
  ) THEN
    ALTER TABLE "search_document_tags"
      ADD CONSTRAINT "search_document_tags_tag_id_fkey"
      FOREIGN KEY ("tag_id") REFERENCES "content_tags"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'channel_default_categories_channel_id_fkey'
  ) THEN
    ALTER TABLE "channel_default_categories"
      ADD CONSTRAINT "channel_default_categories_channel_id_fkey"
      FOREIGN KEY ("channel_id") REFERENCES "channels"("id") ON DELETE CASCADE ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'channel_default_categories_level2_id_fkey'
  ) THEN
    ALTER TABLE "channel_default_categories"
      ADD CONSTRAINT "channel_default_categories_level2_id_fkey"
      FOREIGN KEY ("level2_id") REFERENCES "category_level2"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'channel_default_tags_channel_id_fkey'
  ) THEN
    ALTER TABLE "channel_default_tags"
      ADD CONSTRAINT "channel_default_tags_channel_id_fkey"
      FOREIGN KEY ("channel_id") REFERENCES "channels"("id") ON DELETE CASCADE ON UPDATE CASCADE;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'channel_default_tags_tag_id_fkey'
  ) THEN
    ALTER TABLE "channel_default_tags"
      ADD CONSTRAINT "channel_default_tags_tag_id_fkey"
      FOREIGN KEY ("tag_id") REFERENCES "content_tags"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
  END IF;
END $$;
COMMIT;

CREATE TYPE "CatalogSourceItemType" AS ENUM ('single', 'group');

CREATE TABLE "catalog_source_item" (
  "id" BIGSERIAL NOT NULL,
  "channel_id" BIGINT NOT NULL,
  "telegram_message_id" BIGINT NOT NULL,
  "telegram_message_link" VARCHAR(255),
  "source_type" "CatalogSourceItemType" NOT NULL,
  "group_key" VARCHAR(128),
  "title" VARCHAR(255),
  "caption" TEXT,
  "is_collection" BOOLEAN NOT NULL DEFAULT false,
  "collection_name" VARCHAR(128),
  "episode_no" INTEGER,
  "published_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "source_dispatch_task_id" BIGINT,
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT "catalog_source_item_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "catalog_source_item_channel_id_fkey"
    FOREIGN KEY ("channel_id") REFERENCES "channels"("id")
    ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE UNIQUE INDEX "catalog_source_item_channel_id_telegram_message_id_key"
  ON "catalog_source_item"("channel_id", "telegram_message_id");

CREATE INDEX "catalog_source_item_channel_id_published_at_idx"
  ON "catalog_source_item"("channel_id", "published_at" DESC);

CREATE INDEX "catalog_source_item_channel_id_is_collection_collection_name_episode_no_idx"
  ON "catalog_source_item"("channel_id", "is_collection", "collection_name", "episode_no");

CREATE INDEX "catalog_source_item_channel_id_source_type_published_at_idx"
  ON "catalog_source_item"("channel_id", "source_type", "published_at" DESC);

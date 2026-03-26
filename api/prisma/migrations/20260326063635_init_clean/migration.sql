-- AlterTable
ALTER TABLE "channels" ADD COLUMN     "collection_index_message_id" BIGINT,
ADD COLUMN     "collection_index_page_message_ids" JSONB,
ADD COLUMN     "collection_index_page_size" INTEGER NOT NULL DEFAULT 20,
ADD COLUMN     "collection_nav_enabled" BOOLEAN NOT NULL DEFAULT true,
ADD COLUMN     "last_collection_nav_update_at" TIMESTAMPTZ(6),
ADD COLUMN     "nav_page_message_ids" JSONB,
ADD COLUMN     "nav_page_size" INTEGER NOT NULL DEFAULT 10,
ADD COLUMN     "nav_paging_enabled" BOOLEAN NOT NULL DEFAULT true,
ADD COLUMN     "tags" TEXT[] DEFAULT ARRAY[]::TEXT[];

-- CreateTable
CREATE TABLE "collections" (
    "id" BIGSERIAL NOT NULL,
    "channel_id" BIGINT NOT NULL,
    "name" VARCHAR(128) NOT NULL,
    "slug" VARCHAR(128),
    "dir_path" VARCHAR(255) NOT NULL,
    "cover_asset_id" BIGINT,
    "description" TEXT,
    "status" "ChannelStatus" NOT NULL DEFAULT 'active',
    "sort_order" INTEGER NOT NULL DEFAULT 0,
    "index_message_id" BIGINT,
    "index_page_message_ids" JSONB,
    "last_built_at" TIMESTAMPTZ(6),
    "nav_enabled" BOOLEAN NOT NULL DEFAULT true,
    "nav_page_size" INTEGER NOT NULL DEFAULT 30,
    "template_text" TEXT,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,

    CONSTRAINT "collections_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "collection_episodes" (
    "id" BIGSERIAL NOT NULL,
    "collection_id" BIGINT NOT NULL,
    "media_asset_id" BIGINT NOT NULL,
    "episode_no" INTEGER NOT NULL,
    "episode_title" VARCHAR(255),
    "file_name_snapshot" VARCHAR(255) NOT NULL,
    "parse_status" VARCHAR(16) NOT NULL DEFAULT 'ok',
    "sort_key" VARCHAR(32) NOT NULL,
    "telegram_message_id" BIGINT,
    "telegram_message_link" VARCHAR(255),
    "published_at" TIMESTAMPTZ(6),
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,

    CONSTRAINT "collection_episodes_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "collections_channel_id_status_sort_order_idx" ON "collections"("channel_id", "status", "sort_order");

-- CreateIndex
CREATE INDEX "collections_channel_id_nav_enabled_idx" ON "collections"("channel_id", "nav_enabled");

-- CreateIndex
CREATE UNIQUE INDEX "collections_channel_id_name_key" ON "collections"("channel_id", "name");

-- CreateIndex
CREATE UNIQUE INDEX "collections_channel_id_dir_path_key" ON "collections"("channel_id", "dir_path");

-- CreateIndex
CREATE UNIQUE INDEX "collection_episodes_media_asset_id_key" ON "collection_episodes"("media_asset_id");

-- CreateIndex
CREATE INDEX "collection_episodes_collection_id_episode_no_idx" ON "collection_episodes"("collection_id", "episode_no");

-- CreateIndex
CREATE INDEX "collection_episodes_collection_id_parse_status_idx" ON "collection_episodes"("collection_id", "parse_status");

-- CreateIndex
CREATE INDEX "collection_episodes_published_at_idx" ON "collection_episodes"("published_at");

-- CreateIndex
CREATE UNIQUE INDEX "collection_episodes_collection_id_episode_no_key" ON "collection_episodes"("collection_id", "episode_no");

-- CreateIndex
CREATE INDEX "channels_collection_nav_enabled_idx" ON "channels"("collection_nav_enabled");

-- AddForeignKey
ALTER TABLE "collections" ADD CONSTRAINT "collections_channel_id_fkey" FOREIGN KEY ("channel_id") REFERENCES "channels"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "collection_episodes" ADD CONSTRAINT "collection_episodes_collection_id_fkey" FOREIGN KEY ("collection_id") REFERENCES "collections"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "collection_episodes" ADD CONSTRAINT "collection_episodes_media_asset_id_fkey" FOREIGN KEY ("media_asset_id") REFERENCES "media_assets"("id") ON DELETE CASCADE ON UPDATE CASCADE;

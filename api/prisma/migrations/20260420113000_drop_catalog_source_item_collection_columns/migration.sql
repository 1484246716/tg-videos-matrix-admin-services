DROP INDEX IF EXISTS "catalog_source_item_channel_id_is_collection_collection_name_episode_no_idx";

ALTER TABLE "catalog_source_item"
  DROP COLUMN IF EXISTS "is_collection",
  DROP COLUMN IF EXISTS "collection_name",
  DROP COLUMN IF EXISTS "episode_no";

-- Create collection snapshot head table
CREATE TABLE IF NOT EXISTS "collection_snapshot" (
  "id" BIGSERIAL PRIMARY KEY,
  "channel_id" BIGINT NOT NULL,
  "collection_name" VARCHAR(128) NOT NULL,
  "collection_name_normalized" VARCHAR(128) NOT NULL,
  "episode_count" INTEGER NOT NULL DEFAULT 0,
  "min_episode_no" INTEGER,
  "max_episode_no" INTEGER,
  "last_source_updated_at" TIMESTAMPTZ,
  "last_rebuild_at" TIMESTAMPTZ NOT NULL,
  "version" BIGINT NOT NULL DEFAULT 0,
  "is_deleted" BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE UNIQUE INDEX IF NOT EXISTS "collection_snapshot_channel_id_collection_name_normalized_key"
  ON "collection_snapshot" ("channel_id", "collection_name_normalized");
CREATE INDEX IF NOT EXISTS "collection_snapshot_channel_id_is_deleted_idx"
  ON "collection_snapshot" ("channel_id", "is_deleted");
CREATE INDEX IF NOT EXISTS "collection_snapshot_channel_id_last_rebuild_at_idx"
  ON "collection_snapshot" ("channel_id", "last_rebuild_at" DESC);

-- Create collection episode snapshot table
CREATE TABLE IF NOT EXISTS "collection_episode_snapshot" (
  "id" BIGSERIAL PRIMARY KEY,
  "channel_id" BIGINT NOT NULL,
  "collection_name_normalized" VARCHAR(128) NOT NULL,
  "episode_no" INTEGER NOT NULL,
  "telegram_message_id" BIGINT,
  "telegram_message_url" VARCHAR(255),
  "title" VARCHAR(255),
  "is_missing_placeholder" BOOLEAN NOT NULL DEFAULT FALSE,
  "source_dispatch_task_id" BIGINT,
  "source_updated_at" TIMESTAMPTZ,
  "snapshot_updated_at" TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS "collection_episode_snapshot_channel_id_collection_name_normalized_episode_no_key"
  ON "collection_episode_snapshot" ("channel_id", "collection_name_normalized", "episode_no");
CREATE INDEX IF NOT EXISTS "collection_episode_snapshot_channel_id_collection_name_normalized_episode_no_idx"
  ON "collection_episode_snapshot" ("channel_id", "collection_name_normalized", "episode_no");
CREATE INDEX IF NOT EXISTS "collection_episode_snapshot_channel_id_snapshot_updated_at_idx"
  ON "collection_episode_snapshot" ("channel_id", "snapshot_updated_at" DESC);

-- Create incremental cursor table
CREATE TABLE IF NOT EXISTS "collection_snapshot_cursor" (
  "id" INTEGER PRIMARY KEY,
  "last_dispatch_id" BIGINT NOT NULL DEFAULT 0,
  "updated_at" TIMESTAMPTZ NOT NULL,
  "created_at" TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO "collection_snapshot_cursor" ("id", "last_dispatch_id", "updated_at")
VALUES (1, 0, NOW())
ON CONFLICT ("id") DO NOTHING;

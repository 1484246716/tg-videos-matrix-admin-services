-- CreateTable
CREATE TABLE "search_documents" (
    "id" BIGSERIAL NOT NULL,
    "doc_id" VARCHAR(128) NOT NULL,
    "doc_type" VARCHAR(32) NOT NULL,
    "schema_version" INTEGER NOT NULL DEFAULT 1,
    "channel_id" BIGINT NOT NULL,
    "collection_id" BIGINT,
    "media_asset_id" BIGINT,
    "episode_id" BIGINT,
    "title" VARCHAR(255) NOT NULL,
    "original_title" VARCHAR(255),
    "aliases" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    "actors" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    "directors" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    "genres" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    "keywords" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    "year" INTEGER,
    "region" VARCHAR(64),
    "language" VARCHAR(64),
    "description" TEXT,
    "search_text" TEXT NOT NULL,
    "telegram_message_link" VARCHAR(512),
    "telegram_message_id" BIGINT,
    "published_at" TIMESTAMPTZ(6),
    "quality_score" DECIMAL(8,4) NOT NULL DEFAULT 1.0,
    "popularity_score" DECIMAL(8,4) NOT NULL DEFAULT 0,
    "manual_weight" DECIMAL(8,4) NOT NULL DEFAULT 1.0,
    "visibility" VARCHAR(32) NOT NULL DEFAULT 'public',
    "is_active" BOOLEAN NOT NULL DEFAULT true,
    "is_deleted" BOOLEAN NOT NULL DEFAULT false,
    "ext" JSONB,
    "source_updated_at" TIMESTAMPTZ(6) NOT NULL,
    "indexed_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "search_documents_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "search_index_outbox" (
    "id" BIGSERIAL NOT NULL,
    "doc_id" VARCHAR(128) NOT NULL,
    "op" VARCHAR(16) NOT NULL,
    "payload" JSONB,
    "attempt" INTEGER NOT NULL DEFAULT 0,
    "status" VARCHAR(16) NOT NULL DEFAULT 'pending',
    "next_retry_at" TIMESTAMPTZ(6),
    "last_error" TEXT,
    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMPTZ(6) NOT NULL,

    CONSTRAINT "search_index_outbox_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "search_documents_doc_id_key" ON "search_documents"("doc_id");

-- CreateIndex
CREATE INDEX "search_documents_channel_id_is_active_is_deleted_idx" ON "search_documents"("channel_id", "is_active", "is_deleted");

-- CreateIndex
CREATE INDEX "search_documents_updated_at_idx" ON "search_documents"("updated_at" DESC);

-- CreateIndex
CREATE INDEX "search_index_outbox_status_next_retry_at_created_at_idx" ON "search_index_outbox"("status", "next_retry_at", "created_at");

-- ============================================================
-- 手动追加：tsvector 列 + 触发器 + GIN 索引 (Prisma 不原生支持)
-- ============================================================

-- tsvector 列
ALTER TABLE search_documents ADD COLUMN IF NOT EXISTS search_tsv tsvector;

-- tsvector 自动更新触发器
CREATE OR REPLACE FUNCTION search_documents_tsv_trigger() RETURNS trigger AS $$
BEGIN
  NEW.search_tsv := to_tsvector('simple', COALESCE(NEW.search_text, ''));
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tsvupdate ON search_documents;
CREATE TRIGGER tsvupdate BEFORE INSERT OR UPDATE OF search_text
  ON search_documents FOR EACH ROW EXECUTE FUNCTION search_documents_tsv_trigger();

-- GIN 索引
CREATE INDEX IF NOT EXISTS idx_search_documents_tsv ON search_documents USING GIN(search_tsv);
CREATE INDEX IF NOT EXISTS idx_search_documents_actors ON search_documents USING GIN(actors);
CREATE INDEX IF NOT EXISTS idx_search_documents_aliases ON search_documents USING GIN(aliases);
CREATE INDEX IF NOT EXISTS idx_search_documents_keywords ON search_documents USING GIN(keywords);
CREATE INDEX IF NOT EXISTS idx_search_documents_ext ON search_documents USING GIN(ext);

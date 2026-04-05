-- 方案二：media_assets 路径指纹唯一键迁移
-- 目标：新增 path_normalized/path_fingerprint，并建立 channel_id + path_fingerprint 唯一约束

-- 1) 依赖扩展（用于 sha256）
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 2) 新增字段（先允许为空，便于灰度与回填）
ALTER TABLE "media_assets"
  ADD COLUMN IF NOT EXISTS "path_normalized" TEXT,
  ADD COLUMN IF NOT EXISTS "path_fingerprint" VARCHAR(64);

-- 3) 先补基础索引（非唯一），提升回填与查重性能
CREATE INDEX IF NOT EXISTS "idx_media_assets_channel_path_fp"
  ON "media_assets" ("channel_id", "path_fingerprint");

-- 4) 创建唯一索引（前提：重复脏数据已治理完成）
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND indexname = 'ux_media_assets_channel_path_fp'
  ) THEN
    CREATE UNIQUE INDEX "ux_media_assets_channel_path_fp"
      ON "media_assets" ("channel_id", "path_fingerprint");
  END IF;
END $$;

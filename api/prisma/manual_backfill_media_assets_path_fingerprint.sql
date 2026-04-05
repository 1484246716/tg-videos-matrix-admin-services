-- 方案二（生产防冲突版）：media_assets 路径指纹回填与重复收敛
-- 适用场景：生产库已存在 ux_media_assets_channel_path_fp，直接回填触发 P2002
-- 核心策略：先移除唯一索引 -> 回填 -> 收敛重复 -> 重建唯一索引

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =========================================================
-- 0) 生产安全保护：串行执行，避免多人同时跑脚本
-- =========================================================
SELECT pg_advisory_lock(9223372036854775001);

-- =========================================================
-- 1) 预检查（只读）
-- =========================================================
SELECT
  COUNT(*) AS total_rows,
  COUNT(*) FILTER (WHERE path_fingerprint IS NULL OR path_fingerprint = '') AS null_fp_rows,
  COUNT(*) FILTER (WHERE path_normalized IS NULL OR path_normalized = '') AS null_norm_rows
FROM media_assets;

-- =========================================================
-- 2) 先临时移除唯一索引（避免回填中途触发 P2002）
-- =========================================================
DROP INDEX IF EXISTS "ux_media_assets_channel_path_fp";

-- =========================================================
-- 3) 回填 path_normalized + path_fingerprint
-- 说明：
-- - 使用 lower()，按当前项目 Windows 路径语义统一小写
-- - 统一分隔符、压缩重复斜杠、去尾斜杠
-- =========================================================
UPDATE media_assets
SET
  path_normalized = regexp_replace(
    regexp_replace(lower(replace(trim(local_path), '\\', '/')), '/+', '/', 'g'),
    '/$',
    ''
  ),
  path_fingerprint = encode(
    digest(
      channel_id::text || '|' ||
      regexp_replace(
        regexp_replace(lower(replace(trim(local_path), '\\', '/')), '/+', '/', 'g'),
        '/$',
        ''
      ),
      'sha256'
    ),
    'hex'
  )
WHERE path_fingerprint IS NULL
   OR path_fingerprint = ''
   OR path_normalized IS NULL
   OR path_normalized = '';

-- 回填后空值复查
SELECT
  COUNT(*) FILTER (WHERE path_fingerprint IS NULL OR path_fingerprint = '') AS null_fp_rows_after,
  COUNT(*) FILTER (WHERE path_normalized IS NULL OR path_normalized = '') AS null_norm_rows_after
FROM media_assets;

-- =========================================================
-- 4) 构建主记录-重复记录映射（先落临时表，避免重复计算）
-- 主记录优先级：
--   relay_uploaded/有telegram标识 > ingesting > ready > failed > updated_at desc > id desc
-- =========================================================
DROP TABLE IF EXISTS tmp_media_asset_rank;
CREATE TEMP TABLE tmp_media_asset_rank AS
SELECT
  ma.id,
  ma.channel_id,
  ma.path_fingerprint,
  ma.status,
  ma.telegram_file_id,
  ma.relay_message_id,
  ma.updated_at,
  ROW_NUMBER() OVER (
    PARTITION BY ma.channel_id, ma.path_fingerprint
    ORDER BY
      CASE
        WHEN ma.status = 'relay_uploaded'
          OR ma.telegram_file_id IS NOT NULL
          OR ma.relay_message_id IS NOT NULL THEN 1
        WHEN ma.status = 'ingesting' THEN 2
        WHEN ma.status = 'ready' THEN 3
        WHEN ma.status = 'failed' THEN 4
        ELSE 9
      END,
      ma.updated_at DESC NULLS LAST,
      ma.id DESC
  ) AS rn
FROM media_assets ma
WHERE ma.path_fingerprint IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_tmp_media_asset_rank_id ON tmp_media_asset_rank(id);
CREATE INDEX IF NOT EXISTS idx_tmp_media_asset_rank_group ON tmp_media_asset_rank(channel_id, path_fingerprint, rn);

DROP TABLE IF EXISTS tmp_media_asset_dedup_map;
CREATE TEMP TABLE tmp_media_asset_dedup_map AS
SELECT
  loser.id AS loser_id,
  winner.id AS survivor_id,
  loser.channel_id,
  loser.path_fingerprint
FROM tmp_media_asset_rank loser
JOIN tmp_media_asset_rank winner
  ON winner.channel_id = loser.channel_id
 AND winner.path_fingerprint = loser.path_fingerprint
 AND winner.rn = 1
WHERE loser.rn > 1;

CREATE INDEX IF NOT EXISTS idx_tmp_media_asset_dedup_loser ON tmp_media_asset_dedup_map(loser_id);
CREATE INDEX IF NOT EXISTS idx_tmp_media_asset_dedup_survivor ON tmp_media_asset_dedup_map(survivor_id);

-- 重复规模检查
SELECT COUNT(*) AS duplicate_rows_to_supersede FROM tmp_media_asset_dedup_map;

-- =========================================================
-- 5) 先迁移 dispatch_tasks（防止后续被 loser 继续阻塞）
-- 注意 dispatch_tasks 存在唯一约束 (channel_id, media_asset_id, schedule_slot)
-- 先迁移无冲突任务，再取消冲突任务
-- =========================================================

-- 5.1 无冲突任务迁移
WITH movable AS (
  SELECT
    dt.id AS dispatch_task_id,
    m.survivor_id
  FROM dispatch_tasks dt
  JOIN tmp_media_asset_dedup_map m ON m.loser_id = dt.media_asset_id
  WHERE NOT EXISTS (
    SELECT 1
    FROM dispatch_tasks dt2
    WHERE dt2.channel_id = dt.channel_id
      AND dt2.media_asset_id = m.survivor_id
      AND dt2.schedule_slot = dt.schedule_slot
  )
)
UPDATE dispatch_tasks dt
SET media_asset_id = movable.survivor_id,
    updated_at = NOW()
FROM movable
WHERE dt.id = movable.dispatch_task_id;

-- 5.2 冲突任务取消（由 survivor 任务接管）
WITH conflicted AS (
  SELECT dt.id AS dispatch_task_id
  FROM dispatch_tasks dt
  JOIN tmp_media_asset_dedup_map m ON m.loser_id = dt.media_asset_id
  WHERE EXISTS (
    SELECT 1
    FROM dispatch_tasks dt2
    WHERE dt2.channel_id = dt.channel_id
      AND dt2.media_asset_id = m.survivor_id
      AND dt2.schedule_slot = dt.schedule_slot
  )
)
UPDATE dispatch_tasks dt
SET status = 'cancelled',
    telegram_error_code = COALESCE(dt.telegram_error_code, 'DEDUP_SUPERSEDED'),
    telegram_error_message = COALESCE(dt.telegram_error_message, '')
      || CASE WHEN COALESCE(dt.telegram_error_message, '') = '' THEN '' ELSE ' | ' END
      || '重复资产收敛：任务与主记录冲突，已取消',
    finished_at = COALESCE(dt.finished_at, NOW()),
    updated_at = NOW()
WHERE dt.id IN (SELECT dispatch_task_id FROM conflicted)
  AND dt.status IN ('pending', 'scheduled', 'running', 'failed');

-- =========================================================
-- 6) 标记 loser 资产为 duplicate_superseded
-- =========================================================
UPDATE media_assets ma
SET
  status = CASE WHEN ma.status = 'relay_uploaded' THEN ma.status ELSE 'failed' END,
  ingest_error = COALESCE(ma.ingest_error, '重复资产收敛：已并入主记录'),
  source_meta = (
    COALESCE(ma.source_meta::jsonb, '{}'::jsonb)
    || jsonb_build_object(
      'duplicateOfMediaAssetId', m.survivor_id::text,
      'ingestFinalReason', 'duplicate_superseded',
      'duplicateSupersededAt', NOW()::text
    )
  )::json,
  updated_at = NOW()
FROM tmp_media_asset_dedup_map m
WHERE ma.id = m.loser_id;

-- =========================================================
-- 7) 重建唯一索引前核验
-- =========================================================
SELECT COUNT(*) AS duplicate_group_count_before_recreate_unique
FROM (
  SELECT channel_id, path_fingerprint
  FROM media_assets
  WHERE path_fingerprint IS NOT NULL
  GROUP BY channel_id, path_fingerprint
  HAVING COUNT(*) > 1
) t;

-- =========================================================
-- 8) 重建唯一索引（仅在 duplicate_group_count = 0 时成功）
-- =========================================================
CREATE UNIQUE INDEX IF NOT EXISTS "ux_media_assets_channel_path_fp"
ON "media_assets" ("channel_id", "path_fingerprint");

-- =========================================================
-- 9) 最终核验
-- =========================================================
SELECT COUNT(*) AS duplicate_group_count_after
FROM (
  SELECT channel_id, path_fingerprint
  FROM media_assets
  WHERE path_fingerprint IS NOT NULL
  GROUP BY channel_id, path_fingerprint
  HAVING COUNT(*) > 1
) t;

SELECT COUNT(*) AS superseded_rows
FROM media_assets
WHERE source_meta::jsonb ->> 'ingestFinalReason' = 'duplicate_superseded';

SELECT COUNT(*) AS null_fp_rows_final
FROM media_assets
WHERE path_fingerprint IS NULL OR path_fingerprint = '';

-- =========================================================
-- 10) 释放 advisory lock
-- =========================================================
SELECT pg_advisory_unlock(9223372036854775001);

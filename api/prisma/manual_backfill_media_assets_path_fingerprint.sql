-- 方案二：media_assets 历史数据回填与重复收敛脚本
-- 执行建议：先在测试环境验证，再在生产低峰执行
-- 依赖：PostgreSQL + pgcrypto

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =========================================================
-- A. 预检查（只读）
-- =========================================================

-- A1. 总量与空值
SELECT
  COUNT(*) AS total_rows,
  COUNT(*) FILTER (WHERE path_fingerprint IS NULL OR path_fingerprint = '') AS null_fp_rows,
  COUNT(*) FILTER (WHERE path_normalized IS NULL OR path_normalized = '') AS null_norm_rows
FROM media_assets;

-- A2. 回填后潜在重复组规模（使用临时计算规则预估）
WITH normalized AS (
  SELECT
    id,
    channel_id,
    lower(regexp_replace(replace(trim(local_path), '\\', '/'), '/+', '/', 'g')) AS norm_path,
    encode(digest(channel_id::text || '|' || lower(regexp_replace(replace(trim(local_path), '\\', '/'), '/+', '/', 'g')), 'sha256'), 'hex') AS fp
  FROM media_assets
)
SELECT
  channel_id,
  fp AS path_fingerprint,
  COUNT(*) AS cnt,
  ARRAY_AGG(id ORDER BY id) AS ids
FROM normalized
GROUP BY channel_id, fp
HAVING COUNT(*) > 1
ORDER BY cnt DESC, channel_id;

-- =========================================================
-- B. 回填 path_normalized + path_fingerprint
-- =========================================================

-- 注意：如果你的路径对大小写敏感（Linux 严格区分），可移除 lower()。
UPDATE media_assets
SET
  path_normalized = lower(regexp_replace(replace(trim(local_path), '\\', '/'), '/+', '/', 'g')),
  path_fingerprint = encode(
    digest(
      channel_id::text || '|' || lower(regexp_replace(replace(trim(local_path), '\\', '/'), '/+', '/', 'g')),
      'sha256'
    ),
    'hex'
  )
WHERE path_fingerprint IS NULL
   OR path_fingerprint = ''
   OR path_normalized IS NULL
   OR path_normalized = '';

-- 回填后复查空值
SELECT
  COUNT(*) FILTER (WHERE path_fingerprint IS NULL OR path_fingerprint = '') AS null_fp_rows_after,
  COUNT(*) FILTER (WHERE path_normalized IS NULL OR path_normalized = '') AS null_norm_rows_after
FROM media_assets;

-- =========================================================
-- C. 重复组收敛（保留 1 条主记录，其他标记为 duplicate_superseded）
-- =========================================================

-- 主记录优先级：
-- 1) relay_uploaded / 有 telegram_file_id / 有 relay_message_id
-- 2) ingesting
-- 3) ready
-- 4) failed
-- 5) updated_at 最新

WITH ranked AS (
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
          WHEN ma.status = 'relay_uploaded' OR ma.telegram_file_id IS NOT NULL OR ma.relay_message_id IS NOT NULL THEN 1
          WHEN ma.status = 'ingesting' THEN 2
          WHEN ma.status = 'ready' THEN 3
          WHEN ma.status = 'failed' THEN 4
          ELSE 5
        END,
        ma.updated_at DESC,
        ma.id DESC
    ) AS rn
  FROM media_assets ma
  WHERE ma.path_fingerprint IS NOT NULL
),
survivor AS (
  SELECT channel_id, path_fingerprint, id AS survivor_id
  FROM ranked
  WHERE rn = 1
),
loser AS (
  SELECT r.id AS loser_id, s.survivor_id
  FROM ranked r
  JOIN survivor s
    ON s.channel_id = r.channel_id
   AND s.path_fingerprint = r.path_fingerprint
  WHERE r.rn > 1
)
UPDATE media_assets ma
SET
  status = CASE WHEN ma.status = 'relay_uploaded' THEN ma.status ELSE 'failed' END,
  source_meta = (
    COALESCE(ma.source_meta::jsonb, '{}'::jsonb)
    || jsonb_build_object(
      'duplicateOfMediaAssetId', loser.survivor_id::text,
      'ingestFinalReason', 'duplicate_superseded',
      'duplicateSupersededAt', NOW()::text
    )
  )::json,
  updated_at = NOW()
FROM loser
WHERE ma.id = loser.loser_id;

-- =========================================================
-- D. 迁移 dispatch_tasks 引用到主记录（避免顺序闸门继续被 loser 影响）
-- =========================================================

-- 说明：存在唯一约束 (channel_id, media_asset_id, schedule_slot)
-- 如果迁移后冲突，则保留“优先任务”，其余标记 cancelled + 原因。

WITH ranked AS (
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
          WHEN ma.status = 'relay_uploaded' OR ma.telegram_file_id IS NOT NULL OR ma.relay_message_id IS NOT NULL THEN 1
          WHEN ma.status = 'ingesting' THEN 2
          WHEN ma.status = 'ready' THEN 3
          WHEN ma.status = 'failed' THEN 4
          ELSE 5
        END,
        ma.updated_at DESC,
        ma.id DESC
    ) AS rn
  FROM media_assets ma
  WHERE ma.path_fingerprint IS NOT NULL
),
survivor AS (
  SELECT channel_id, path_fingerprint, id AS survivor_id
  FROM ranked
  WHERE rn = 1
),
loser AS (
  SELECT r.id AS loser_id, s.survivor_id
  FROM ranked r
  JOIN survivor s
    ON s.channel_id = r.channel_id
   AND s.path_fingerprint = r.path_fingerprint
  WHERE r.rn > 1
),
conflict_slots AS (
  SELECT
    dt.channel_id,
    dt.schedule_slot,
    l.survivor_id,
    COUNT(*) AS cnt
  FROM dispatch_tasks dt
  JOIN loser l ON l.loser_id = dt.media_asset_id
  GROUP BY dt.channel_id, dt.schedule_slot, l.survivor_id
  HAVING COUNT(*) > 0
),
to_cancel AS (
  SELECT dt.id
  FROM dispatch_tasks dt
  JOIN loser l ON l.loser_id = dt.media_asset_id
  JOIN dispatch_tasks d2
    ON d2.channel_id = dt.channel_id
   AND d2.schedule_slot = dt.schedule_slot
   AND d2.media_asset_id = l.survivor_id
)
UPDATE dispatch_tasks dt
SET
  status = 'cancelled',
  telegram_error_message = COALESCE(dt.telegram_error_message, '') || ' | duplicate_superseded 任务迁移时与主记录冲突，已取消',
  updated_at = NOW()
WHERE dt.id IN (SELECT id FROM to_cancel)
  AND dt.status IN ('pending','scheduled','running','failed');

-- 非冲突任务迁移 media_asset_id -> survivor_id
WITH ranked AS (
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
          WHEN ma.status = 'relay_uploaded' OR ma.telegram_file_id IS NOT NULL OR ma.relay_message_id IS NOT NULL THEN 1
          WHEN ma.status = 'ingesting' THEN 2
          WHEN ma.status = 'ready' THEN 3
          WHEN ma.status = 'failed' THEN 4
          ELSE 5
        END,
        ma.updated_at DESC,
        ma.id DESC
    ) AS rn
  FROM media_assets ma
  WHERE ma.path_fingerprint IS NOT NULL
),
survivor AS (
  SELECT channel_id, path_fingerprint, id AS survivor_id
  FROM ranked
  WHERE rn = 1
),
loser AS (
  SELECT r.id AS loser_id, s.survivor_id
  FROM ranked r
  JOIN survivor s
    ON s.channel_id = r.channel_id
   AND s.path_fingerprint = r.path_fingerprint
  WHERE r.rn > 1
),
can_move AS (
  SELECT dt.id, l.survivor_id
  FROM dispatch_tasks dt
  JOIN loser l ON l.loser_id = dt.media_asset_id
  LEFT JOIN dispatch_tasks d2
    ON d2.channel_id = dt.channel_id
   AND d2.schedule_slot = dt.schedule_slot
   AND d2.media_asset_id = l.survivor_id
  WHERE d2.id IS NULL
)
UPDATE dispatch_tasks dt
SET media_asset_id = can_move.survivor_id,
    updated_at = NOW()
FROM can_move
WHERE dt.id = can_move.id;

-- =========================================================
-- E. 最终核验
-- =========================================================

-- E1. 是否仍有重复组
SELECT COUNT(*) AS duplicate_group_count
FROM (
  SELECT channel_id, path_fingerprint
  FROM media_assets
  WHERE path_fingerprint IS NOT NULL
  GROUP BY channel_id, path_fingerprint
  HAVING COUNT(*) > 1
) t;

-- E2. 重复收敛标记数量
SELECT COUNT(*) AS superseded_rows
FROM media_assets
WHERE source_meta::jsonb ->> 'ingestFinalReason' = 'duplicate_superseded';

-- E3. 可选：查看最新 100 条收敛记录
SELECT id, channel_id, local_path, status, source_meta, updated_at
FROM media_assets
WHERE source_meta::jsonb ? 'duplicateOfMediaAssetId'
ORDER BY updated_at DESC
LIMIT 100;

-- =========================================================
-- F. 注意事项
-- =========================================================
-- 1) 唯一索引创建前，必须确认 E1 = 0。
-- 2) 建议配合业务低峰执行，且先备份。
-- 3) 若需分批回填，可按 id 范围加 WHERE 条件分段执行。

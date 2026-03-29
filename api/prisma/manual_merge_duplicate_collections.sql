-- 手工治理脚本：合并同频道下“疑似改名重复合集”
-- 使用前请先备份数据库，并在低峰期执行。

BEGIN;

-- 1) 识别重复合集（同频道 + 归一化名称）
WITH normalized AS (
  SELECT
    c.id,
    c.channel_id,
    c.name,
    regexp_replace(trim(c.name), '\\s+', ' ', 'g') AS norm_name,
    c.created_at
  FROM collections c
), dup_groups AS (
  SELECT channel_id, norm_name, COUNT(*) AS cnt
  FROM normalized
  GROUP BY channel_id, norm_name
  HAVING COUNT(*) > 1
), ranked AS (
  SELECT
    n.*,
    ROW_NUMBER() OVER (PARTITION BY n.channel_id, n.norm_name ORDER BY n.created_at ASC, n.id ASC) AS rn
  FROM normalized n
  INNER JOIN dup_groups g
    ON g.channel_id = n.channel_id
   AND g.norm_name = n.norm_name
)
-- 2) 将重复合集( rn > 1 )的剧集迁移到主合集( rn = 1 )
UPDATE collection_episodes ce
SET collection_id = keeper.id
FROM ranked dup
JOIN ranked keeper
  ON keeper.channel_id = dup.channel_id
 AND keeper.norm_name = dup.norm_name
 AND keeper.rn = 1
WHERE ce.collection_id = dup.id
  AND dup.rn > 1;

-- 3) 删除重复合集(仅保留 keeper)
DELETE FROM collections c
USING ranked dup
WHERE c.id = dup.id
  AND dup.rn > 1;

COMMIT;

-- 可选：执行后触发频道目录刷新（由应用侧批量触发 q_catalog 更稳妥）

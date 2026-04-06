BEGIN;

-- 一级分类（幂等）
INSERT INTO category_level1 (name, slug, sort, status, created_at, updated_at)
VALUES
  ('电视剧/剧集', 'tv-series', 10, 'active', now(), now()),
  ('电影', 'movie', 20, 'active', now(), now()),
  ('动漫', 'anime', 30, 'active', now(), now()),
  ('综艺', 'variety', 40, 'active', now(), now()),
  ('游戏', 'game', 50, 'active', now(), now()),
  ('纪录片', 'documentary', 60, 'active', now(), now()),
  ('短剧', 'short-drama', 70, 'active', now(), now())
ON CONFLICT (name)
DO UPDATE SET
  slug = EXCLUDED.slug,
  sort = EXCLUDED.sort,
  status = EXCLUDED.status,
  updated_at = now();

-- 二级分类（幂等）
WITH l1 AS (
  SELECT id, name FROM category_level1
), seed(level1_name, level2_name, level2_slug, sort_no) AS (
  VALUES
  -- 电视剧/剧集
  ('电视剧/剧集','爱情','tv-love',1),
  ('电视剧/剧集','都市','tv-urban',2),
  ('电视剧/剧集','青春','tv-youth',3),
  ('电视剧/剧集','奇幻','tv-fantasy',4),
  ('电视剧/剧集','武侠','tv-wuxia',5),
  ('电视剧/剧集','古装','tv-costume',6),
  ('电视剧/剧集','科幻','tv-sci-fi',7),
  ('电视剧/剧集','猎奇','tv-bizarre',8),
  ('电视剧/剧集','竞技','tv-competition',9),
  ('电视剧/剧集','传奇','tv-legend',10),
  ('电视剧/剧集','逆袭','tv-rise',11),
  ('电视剧/剧集','军旅','tv-military',12),
  ('电视剧/剧集','家庭','tv-family',13),
  ('电视剧/剧集','喜剧','tv-comedy',14),
  ('电视剧/剧集','悬疑','tv-suspense',15),
  ('电视剧/剧集','权谋','tv-power-struggle',16),
  ('电视剧/剧集','革命','tv-revolution',17),
  ('电视剧/剧集','现实','tv-realism',18),
  ('电视剧/剧集','刑侦','tv-crime-investigation',19),
  ('电视剧/剧集','民国','tv-republic-era',20),
  ('电视剧/剧集','IP改编','tv-ip-adaptation',21),

  -- 电影
  ('电影','动作','movie-action',1),
  ('电影','喜剧','movie-comedy',2),
  ('电影','爱情','movie-romance',3),
  ('电影','科幻','movie-sci-fi',4),
  ('电影','犯罪','movie-crime',5),
  ('电影','冒险','movie-adventure',6),
  ('电影','恐怖','movie-horror',7),
  ('电影','动画','movie-animation',8),
  ('电影','战争','movie-war',9),
  ('电影','悬疑','movie-suspense',10),
  ('电影','灾难','movie-disaster',11),
  ('电影','青春','movie-youth',12),

  -- 动漫
  ('动漫','玄幻','anime-xuanhuan',1),
  ('动漫','科幻','anime-sci-fi',2),
  ('动漫','奇幻','anime-fantasy',3),
  ('动漫','武侠','anime-wuxia',4),
  ('动漫','仙侠','anime-xianxia',5),
  ('动漫','都市','anime-urban',6),
  ('动漫','恋爱','anime-romance',7),
  ('动漫','搞笑','anime-funny',8),
  ('动漫','冒险','anime-adventure',9),
  ('动漫','悬疑','anime-suspense',10),
  ('动漫','竞技','anime-competition',11),
  ('动漫','日常','anime-daily',12),
  ('动漫','真人','anime-live-action',13),
  ('动漫','治愈','anime-healing',14),
  ('动漫','游戏','anime-game',15),
  ('动漫','异能','anime-superpower',16),
  ('动漫','历史','anime-history',17),
  ('动漫','古风','anime-ancient-style',18),
  ('动漫','智斗','anime-mind-game',19),
  ('动漫','恐怖','anime-horror',20),
  ('动漫','美食','anime-food',21),
  ('动漫','音乐','anime-music',22),

  -- 综艺
  ('综艺','游戏','variety-game',1),
  ('综艺','脱口秀','variety-talk-show',2),
  ('综艺','音乐舞台','variety-music-stage',3),
  ('综艺','情感','variety-emotion',4),
  ('综艺','生活','variety-life',5),
  ('综艺','职场','variety-workplace',6),
  ('综艺','喜剧','variety-comedy',7),
  ('综艺','美食','variety-food',8),
  ('综艺','潮流运动','variety-trendy-sports',9),
  ('综艺','竞技','variety-competition',10),
  ('综艺','影视','variety-film-tv',11),
  ('综艺','电竞','variety-esports',12),
  ('综艺','推理','variety-reasoning',13),
  ('综艺','访谈','variety-interview',14),
  ('综艺','亲子','variety-parenting',15),

  -- 纪录片
  ('纪录片','自然','doc-nature',1),
  ('纪录片','历史','doc-history',2),
  ('纪录片','人文','doc-humanity',3),
  ('纪录片','美食','doc-food',4),
  ('纪录片','医疗','doc-medical',5),
  ('纪录片','萌宠','doc-pets',6),
  ('纪录片','财经','doc-finance',7),
  ('纪录片','罪案','doc-crime',8),
  ('纪录片','竞技','doc-competition',9),
  ('纪录片','灾难','doc-disaster',10),
  ('纪录片','军事','doc-military',11),
  ('纪录片','探险','doc-expedition',12),
  ('纪录片','社会','doc-society',13),
  ('纪录片','科技','doc-technology',14),
  ('纪录片','旅游','doc-travel',15),

  -- 短剧
  ('短剧','穿越','short-time-travel',1),
  ('短剧','逆袭','short-rise',2),
  ('短剧','重生','short-rebirth',3),
  ('短剧','爱情','short-romance',4),
  ('短剧','玄幻','short-xuanhuan',5),
  ('短剧','虐恋','short-angst-romance',6),
  ('短剧','甜宠','short-sweet-romance',7),
  ('短剧','神豪','short-rich-hero',8),
  ('短剧','女性成长','short-female-growth',9),
  ('短剧','古风权谋','short-ancient-politics',10),
  ('短剧','家庭伦理','short-family-ethics',11),
  ('短剧','复仇','short-revenge',12),
  ('短剧','悬疑','short-suspense',13),
  ('短剧','生活','short-life',14),
  ('短剧','刑侦','short-crime-investigation',15),
  ('短剧','恐怖','short-horror',16),

  -- 游戏
  ('游戏','手游','game-mobile',1),
  ('游戏','端游','game-pc',2),
  ('游戏','电竞','game-esports',3),
  ('游戏','剧情','game-story',4),
  ('游戏','沙盒','game-sandbox',5),
  ('游戏','射击','game-shooter',6),
  ('游戏','策略','game-strategy',7),
  ('游戏','卡牌','game-card',8),
  ('游戏','模拟','game-simulation',9),
  ('游戏','动作','game-action',10),
  ('游戏','冒险','game-adventure',11)
)
INSERT INTO category_level2 (level1_id, name, slug, sort, status, created_at, updated_at)
SELECT
  l1.id,
  seed.level2_name,
  seed.level2_slug,
  seed.sort_no,
  'active',
  now(),
  now()
FROM seed
JOIN l1 ON l1.name = seed.level1_name
ON CONFLICT (level1_id, name)
DO UPDATE SET
  slug = EXCLUDED.slug,
  sort = EXCLUDED.sort,
  status = EXCLUDED.status,
  updated_at = now();

COMMIT;

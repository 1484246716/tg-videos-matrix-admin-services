BEGIN;

/*
  合规初始化脚本（可重复执行）
  - 覆盖：category_level1 / category_level2 / content_tags
  - 仅写入可运营、合规词；高风险词（未成年人暗示、非自愿暴力、隐私侵害等）不入库
*/

-- =========================
-- 1) 一级分类
-- =========================
INSERT INTO category_level1 (name, slug, sort, status, created_at, updated_at)
VALUES
  ('电视剧/剧集', 'tv-series', 10, 'active', now(), now()),
  ('电影', 'movie', 20, 'active', now(), now()),
  ('动漫', 'anime', 30, 'active', now(), now()),
  ('综艺', 'variety', 40, 'active', now(), now()),
  ('游戏', 'game', 50, 'active', now(), now()),
  ('纪录片', 'documentary', 60, 'active', now(), now()),
  ('短剧', 'short-drama', 70, 'active', now(), now()),
  ('成人色情', 'adult-erotic', 80, 'active', now(), now()),
  ('成人短视频', 'adult-short-video', 81, 'active', now(), now())
ON CONFLICT (name)
DO UPDATE SET
  slug = EXCLUDED.slug,
  sort = EXCLUDED.sort,
  status = EXCLUDED.status,
  updated_at = now();

-- =========================
-- 2) 二级分类
-- =========================
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
  ('游戏','冒险','game-adventure',11),

  -- 成人色情（合规版）
  ('成人色情','亚洲无码','adult-asia',1),
  ('成人色情','欧美无码','adult-western',2),
  ('成人色情','国产主播','adult-cn-host',3),
  ('成人色情','中文字幕','adult-subtitle-zh',4),
  ('成人色情','韩国主播','adult-kr-host',5),
  ('成人色情','ASMR','adult-asmr',6),
  ('成人色情','经典三级','adult-story-series',7),
  ('成人色情','动漫卡通','adult-animation',8),

  -- 成人短视频（合规版）
  ('成人短视频','探花约炮','adult-short-hookup',1),
  ('成人短视频','国产视频','adult-short-domestic',2),
  ('成人短视频','福利姬','adult-short-benefit-host',3),
  ('成人短视频','人妖伪娘','adult-short-androgynous',4)
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

-- =========================
-- 3) 标签（content_tags）
-- =========================
INSERT INTO content_tags (name, slug, sort, status, scope, created_at, updated_at)
VALUES
  ('3P', 'tag-3p', 10, 'active', 'adult_18', now(), now()),
  ('69', 'tag-69', 20, 'active', 'adult_18', now(), now()),
  ('OK', 'tag-ok', 30, 'active', 'adult_18', now(), now()),
  ('OL', 'tag-ol', 40, 'active', 'adult_18', now(), now()),
  ('SM', 'tag-sm', 50, 'active', 'adult_18', now(), now()),
  ('办公室女士', 'office-lady', 60, 'active', 'adult_18', now(), now()),
  ('护士', 'nurse', 70, 'active', 'adult_18', now(), now()),
  ('教师', 'teacher', 80, 'active', 'adult_18', now(), now()),
  ('女仆', 'maid', 90, 'active', 'adult_18', now(), now()),
  ('学生', 'student', 100, 'active', 'adult_18', now(), now()),
  ('少女', 'young-woman', 110, 'active', 'adult_18', now(), now()),
  ('熟女', 'mature-woman', 120, 'active', 'adult_18', now(), now()),
  ('少妇', 'young-married-woman', 130, 'active', 'adult_18', now(), now()),
  ('人妻', 'married-woman', 140, 'active', 'adult_18', now(), now()),
  ('女神', 'goddess', 150, 'active', 'adult_18', now(), now()),
  ('留学生', 'international-student', 160, 'active', 'adult_18', now(), now()),
  ('素人', 'amateur', 170, 'active', 'adult_18', now(), now()),
  ('空姐', 'flight-attendant', 180, 'active', 'adult_18', now(), now()),
  ('业余', 'part-time', 190, 'active', 'adult_18', now(), now()),
  ('办公室', 'office', 200, 'active', 'adult_18', now(), now()),
  ('角色扮演', 'roleplay-scene', 210, 'active', 'adult_18', now(), now()),
  ('户外', 'outdoor', 220, 'active', 'adult_18', now(), now()),
  ('公共', 'public', 230, 'active', 'adult_18', now(), now()),
  ('健身', 'fitness', 240, 'active', 'adult_18', now(), now()),
  ('剧情', 'storyline', 250, 'active', 'adult_18', now(), now()),
  ('出轨', 'affair', 260, 'active', 'adult_18', now(), now()),
  ('偷情', 'cheating', 270, 'active', 'adult_18', now(), now()),
  ('调教', 'training', 280, 'active', 'adult_18', now(), now()),
  ('捆绑', 'bondage', 290, 'active', 'adult_18', now(), now()),
  ('虐恋', 'bdsm-romance', 300, 'active', 'adult_18', now(), now()),
  ('白虎', 'shaved', 310, 'active', 'adult_18', now(), now()),
  ('巨乳', 'large-breasts', 320, 'active', 'adult_18', now(), now()),
  ('大奶', 'big-boobs', 330, 'active', 'adult_18', now(), now()),
  ('乳交', 'paizuri', 340, 'active', 'adult_18', now(), now()),
  ('大屁股', 'big-butt', 350, 'active', 'adult_18', now(), now()),
  ('美臀', 'nice-butt', 360, 'active', 'adult_18', now(), now()),
  ('美腿控', 'leg-fetish', 370, 'active', 'adult_18', now(), now()),
  ('喷水', 'squirting', 380, 'active', 'adult_18', now(), now()),
  ('口交', 'oral-sex', 390, 'active', 'adult_18', now(), now()),
  ('肛交', 'anal-sex', 400, 'active', 'adult_18', now(), now()),
  ('拳交', 'fisting', 410, 'active', 'adult_18', now(), now()),
  ('群P', 'group-sex', 420, 'active', 'adult_18', now(), now()),
  ('射精', 'ejaculation', 430, 'active', 'adult_18', now(), now()),
  ('内射', 'internal-ejaculation', 440, 'active', 'adult_18', now(), now()),
  ('无套内射', 'creampie-without-condom', 450, 'active', 'adult_18', now(), now()),
  ('无套中出', 'finish-inside-without-condom', 460, 'active', 'adult_18', now(), now()),
  ('中出', 'finish-inside', 470, 'active', 'adult_18', now(), now()),
  ('颜射', 'facial', 480, 'active', 'adult_18', now(), now()),
  ('自慰', 'masturbation', 490, 'active', 'adult_18', now(), now()),
  ('精液自慰', 'semen-masturbation-technique', 500, 'active', 'adult_18', now(), now()),
  ('制服', 'uniform', 510, 'active', 'adult_18', now(), now()),
  ('内衣', 'underwear', 520, 'active', 'adult_18', now(), now()),
  ('丝袜', 'stockings', 530, 'active', 'adult_18', now(), now()),
  ('黑丝', 'black-stockings', 540, 'active', 'adult_18', now(), now()),
  ('白丝', 'white-stockings', 550, 'active', 'adult_18', now(), now()),
  ('高跟鞋', 'high-heels', 560, 'active', 'adult_18', now(), now()),
  ('道具', 'props', 570, 'active', 'adult_18', now(), now()),
  ('精液润滑剂', 'semen-lubricant', 580, 'active', 'adult_18', now(), now())
ON CONFLICT (name)
DO UPDATE SET
  slug = EXCLUDED.slug,
  sort = EXCLUDED.sort,
  status = EXCLUDED.status,
  scope = EXCLUDED.scope,
  updated_at = now();

COMMIT;

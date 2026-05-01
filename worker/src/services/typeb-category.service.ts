/**
 * ?????TypeB ??????????? AI ?????????/???????
 * ?????dispatch.service ????????? -> AI ?? -> media_asset_categories ???
 */

import { AiModelProfile } from '@prisma/client';
import { prisma } from '../infra/prisma';
import { logger } from '../logger';
import { generateTextWithAiProfile } from '../ai-provider';

const CATEGORY_MAP: Record<string, string[]> = {
  '电视剧/剧集': ['爱情', '都市', '青春', '奇幻', '武侠', '古装', '科幻', '猎奇', '竞技', '传奇', '逆袭', '军旅', '家庭', '喜剧', '悬疑', '权谋', '革命', '现实', '刑侦', '民国', 'IP改编'],
  '电影': ['动作', '喜剧', '爱情', '科幻', '犯罪', '冒险', '恐怖', '动画', '战争', '悬疑', '灾难', '青春'],
  '动漫': ['玄幻', '科幻', '奇幻', '武侠', '仙侠', '都市', '恋爱', '搞笑', '冒险', '悬疑', '竞技', '日常', '真人', '治愈', '游戏', '异能', '历史', '古风', '智斗', '恐怖', '美食', '音乐'],
  '综艺': ['游戏', '脱口秀', '音乐舞台', '情感', '生活', '职场', '喜剧', '美食', '潮流运动', '竞技', '影视', '电竞', '推理', '访谈', '亲子'],
  '纪录片': ['自然', '历史', '人文', '美食', '医疗', '萌宠', '财经', '罪案', '竞技', '灾难', '军事', '探险', '社会', '科技', '旅游'],
  '短剧': ['穿越', '逆袭', '重生', '爱情', '玄幻', '虐恋', '甜宠', '神豪', '女性成长', '古风权谋', '家庭伦理', '复仇', '悬疑', '生活', '刑侦', '恐怖'],
  '游戏': ['手游', '端游', '电竞', '剧情', '沙盒', '射击', '策略', '卡牌', '模拟', '动作', '冒险'],
  '成人色情': ['亚洲无码', '欧美无码', '国产主播', '中文字幕', '韩国主播', 'ASMR', '经典三级', '恐怖色情', '动漫卡通'],
  '成人短视频': ['探花约炮', '国产视频', '福利姬', '人妖伪娘'],
};

const LEVEL1_NAMES = Object.keys(CATEGORY_MAP);
const ADULT_LEVEL1_NAMES = new Set(['成人色情', '成人短视频']);
const SHORT_ADULT_VIDEO_MAX_DURATION_SEC = 480;
const ADULT_EXPLICIT_KEYWORDS = [
  '3p',
  '69',
  'ok',
  'ol',
  'sm',
  '办公室女士',
  '护士',
  '教师',
  '老师',
  '女仆',
  '学生',
  '少女',
  '抽插',
  '穴',
  '阴道',
  '阴唇',
  '奶子',
  '乳头',
  '高潮',
  '白浆',
  '喷精',
  '潮吹',
  '内射',
  '中出',
  '口交',
  '肛交',
  '后入',
  '自慰',
  '浪叫',
  '呻吟',
  '做爱',
  '啪啪',
  '精液',
  '龟头',
  '鸡巴',
  '肉棒',
  '爆乳',
  '熟女',
  '少妇',
  '人妻',
  '女神',
  '留学生',
  '素人',
  '空姐',
  '业余',
  '办公室',
  '角色扮演',
  '户外',
  '公共',
  '健身',
  '出轨',
  '偷情',
  '调教',
  '捆绑',
  '虐恋',
  '白虎',
  '巨乳',
  '大奶',
  '乳交',
  '大屁股',
  '美臀',
  '美腿控',
  '喷水',
  '肛交',
  '拳交',
  '群p',
  '群交',
  '射精',
  '无套内射',
  '无套中出',
  '中出',
  '颜射',
  '精液自慰',
  '指法',
  '制服',
  '内衣',
  '丝袜',
  '黑丝',
  '白丝',
  '高跟鞋',
  '道具',
  '精液润滑剂',
];
const ADULT_SHORT_VIDEO_KEYWORDS = [
  '探花',
  '约炮',
  '福利姬',
  '短视频',
  '竖屏',
  '私拍',
  '自拍',
  '网红',
  '主播',
  '女主播',
  '抖音',
  '快手',
  '露脸',
  '黑丝',
  '白丝',
  '制服',
  '丝袜',
  '高跟鞋',
  'ol',
  '角色扮演',
  '业余',
  '素人',
];
// 这里只是成人一级分类已经成立后的二级类目提示，不应单独把普通内容打进成人分类。
const ADULT_LEVEL2_HINT_RULES: Array<{ level1: '成人色情' | '成人短视频'; level2: string; keywords: string[] }> = [
  { level1: '成人短视频', level2: '探花约炮', keywords: ['探花', '约炮'] },
  { level1: '成人短视频', level2: '福利姬', keywords: ['福利姬', '网红', '黑丝', '白丝', '制服', '丝袜', '高跟鞋', '女神', 'ol', '萝莉'] },
  { level1: '成人短视频', level2: '人妖伪娘', keywords: ['人妖', '伪娘', 'ts'] },
  { level1: '成人短视频', level2: '国产视频', keywords: ['国产', '自拍', '私拍', '露脸', '素人', '业余', '学生', '教师', '护士', '空姐', '女仆'] },
  { level1: '成人色情', level2: 'ASMR', keywords: ['asmr'] },
  { level1: '成人色情', level2: '中文字幕', keywords: ['中文字幕', '中字', '字幕'] },
  { level1: '成人色情', level2: '动漫卡通', keywords: ['动漫', '卡通', '二次元'] },
  { level1: '成人色情', level2: '韩国主播', keywords: ['韩国', '韩系'] },
  { level1: '成人色情', level2: '欧美无码', keywords: ['欧美', '白人', '金发'] },
  { level1: '成人色情', level2: '国产主播', keywords: ['主播', '女主播', '麻豆', '国产', 'ol', '办公室女士', '护士', '教师', '女仆', '空姐', '偷拍'] },
  { level1: '成人色情', level2: '经典三级', keywords: ['剧情', '系列', '合集', '角色扮演', '办公室', '户外', '公厕', '公共', '乱伦', '强奸'] },
  { level1: '成人色情', level2: '恐怖色情', keywords: ['sm', '调教', '捆绑', '虐恋', '鞭刑'] },
  { level1: '成人色情', level2: '亚洲无码', keywords: ['无码'] },
];

type ForcedAdultCategoryHint = {
  level1: '成人色情' | '成人短视频';
  level2: string;
  confidence: number;
  matchedKeywords: string[];
  reason: string;
};

// ?? slugify ?????????????????????
function slugify(input: string) {
  return input
    .toLowerCase()
    .replace(/\s+/g, '-')
    .replace(/[^\w\-\u4e00-\u9fa5]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

// ?? parse AI Json ????????????????????????
function parseAiJson(text: string): { level1?: string; level2?: string[]; confidence?: number; reason?: string } {
  const trimmed = text.trim();
  const block = trimmed.match(/\{[\s\S]*\}/)?.[0] || trimmed;
  return JSON.parse(block);
}

function normalizeCategoryText(text: string | null | undefined) {
  return String(text || '').toLowerCase();
}

function detectForcedAdultCategoryHint(args: {
  originalName: string;
  aiCaption?: string | null;
  durationSec?: number | null;
}): ForcedAdultCategoryHint | null {
  const text = normalizeCategoryText(`${args.originalName}\n${args.aiCaption || ''}`);
  const matchedExplicitKeywords = ADULT_EXPLICIT_KEYWORDS.filter((keyword) =>
    text.includes(keyword.toLowerCase()),
  );

  // 先命中成人强信号，再允许用“动漫/卡通/二次元”等二级类目词做细分，避免普通动漫误判为成人。
  if (matchedExplicitKeywords.length === 0) {
    return null;
  }

  const matchedShortVideoKeywords = ADULT_SHORT_VIDEO_KEYWORDS.filter((keyword) =>
    text.includes(keyword.toLowerCase()),
  );
  const isShortAdultVideo =
    typeof args.durationSec === 'number' &&
    Number.isFinite(args.durationSec) &&
    args.durationSec > 0 &&
    args.durationSec <= SHORT_ADULT_VIDEO_MAX_DURATION_SEC;

  const level1: ForcedAdultCategoryHint['level1'] =
    matchedShortVideoKeywords.length > 0 || isShortAdultVideo ? '成人短视频' : '成人色情';
  const fallbackLevel2 = level1 === '成人短视频' ? '国产视频' : '亚洲无码';
  const level2 =
    ADULT_LEVEL2_HINT_RULES.find(
      (rule) =>
        rule.level1 === level1 &&
        rule.keywords.some((keyword) => text.includes(keyword.toLowerCase())),
    )?.level2 ?? fallbackLevel2;
  const matchedKeywords = Array.from(
    new Set([...matchedExplicitKeywords, ...matchedShortVideoKeywords, ...(isShortAdultVideo ? ['短时长视频'] : [])]),
  ).slice(0, 6);

  return {
    level1,
    level2,
    confidence: 0.98,
    matchedKeywords,
    reason: `命中成人内容强信号词：${matchedKeywords.join('、')}`,
  };
}

// ?? ensure Level1 ????????????????????????
async function ensureLevel1(name: string): Promise<bigint> {
  const slug = slugify(name) || `l1-${Date.now()}`;
  const rows = await prisma.$queryRawUnsafe<Array<{ id: bigint }>>(
    `
    INSERT INTO category_level1(name, slug, sort, status, created_at, updated_at)
    VALUES ($1, $2, 0, 'active', now(), now())
    ON CONFLICT (name) DO UPDATE SET updated_at = now()
    RETURNING id
  `,
    name,
    slug,
  );

  if (rows.length > 0) return rows[0].id;

  const found = await prisma.$queryRawUnsafe<Array<{ id: bigint }>>(
    `SELECT id FROM category_level1 WHERE name = $1 LIMIT 1`,
    name,
  );
  if (found.length === 0) throw new Error(`未找到一级分类: ${name}`);
  return found[0].id;
}

// ?? ensure Level2 ????????????????????????
async function ensureLevel2(level1Id: bigint, name: string): Promise<bigint> {
  const slug = `${slugify(String(level1Id))}-${slugify(name)}`;
  const rows = await prisma.$queryRawUnsafe<Array<{ id: bigint }>>(
    `
    INSERT INTO category_level2(level1_id, name, slug, sort, status, created_at, updated_at)
    VALUES ($1, $2, $3, 0, 'active', now(), now())
    ON CONFLICT (level1_id, name) DO UPDATE SET updated_at = now()
    RETURNING id
  `,
    level1Id,
    name,
    slug,
  );

  if (rows.length > 0) return rows[0].id;

  const found = await prisma.$queryRawUnsafe<Array<{ id: bigint }>>(
    `SELECT id FROM category_level2 WHERE level1_id = $1 AND name = $2 LIMIT 1`,
    level1Id,
    name,
  );
  if (found.length === 0) throw new Error(`未找到二级分类: ${name}`);
  return found[0].id;
}

// ?? TypeB ?????????????????????????
export async function classifyAndAssignForTypeB(args: {
  mediaAssetId: bigint;
  originalName: string;
  aiCaption: string;
  durationSec?: number | null;
  profile: Pick<AiModelProfile, 'endpointUrl' | 'apiKeyEncrypted' | 'model' | 'temperature' | 'topP' | 'maxTokens'>;
}) {
  const forcedAdultHint = detectForcedAdultCategoryHint({
    originalName: args.originalName,
    aiCaption: args.aiCaption,
    durationSec: args.durationSec,
  });

  logger.info('[typeb_category] 开始自动生成分类', {
    mediaAssetId: args.mediaAssetId.toString(),
    originalName: args.originalName,
    hasCaption: Boolean(args.aiCaption),
    forcedAdultLevel1: forcedAdultHint?.level1 ?? null,
    forcedAdultLevel2: forcedAdultHint?.level2 ?? null,
  });
  const taxonomyText = LEVEL1_NAMES.map((l1) => `${l1}: ${CATEGORY_MAP[l1].join('、')}`).join('\n');

  const systemPrompt = [
    '你是视频分类器，只能基于输入信息在给定分类体系中选择。',
    '必须输出严格 JSON，禁止输出 Markdown、代码块、额外解释。',
    'JSON格式: {"level1":"...","level2":["..."],"confidence":0.0,"reason":"..."}',
    '一级分类判定规则（按优先级执行）：',
    '1) 若输入出现明确性行为、性器官、高潮射精、露骨性描写等成人强信号，一级分类必须优先判定为【成人色情】或【成人短视频】；不要降级判成【电影/爱情】等普通类目。',
    '2) 若出现“探花/约炮/福利姬/主播/私拍/自拍/竖屏短视频/网红”等短视频强信号，或视频时长明显较短，则优先判定为【成人短视频】。',
    '3) 若出现“小品/春晚小品/相声/赵本山/宋丹丹/范伟/小沈阳”等强信号，优先判定为【综艺】。',
    '4) 只有明确出现“连续剧/剧集/第X季/连载剧情”等长剧情信号时，才判定为【电视剧/剧集】。',
    '5) 只有明确出现“短剧/微短剧/男频女频短剧”等非成人短剧信号时，才判定为【短剧】。',
    '6) 不要因为“第X集”就默认判定为【电视剧/剧集】；合集分集的小品仍可属于【综艺】。',
    '7) level2 必须从对应 level1 的可选项中选择 1-3 个，若不确定优先选择语义最稳妥的单个标签。',
    '8) confidence 取值 0-1；reason 用一句中文简述依据，需引用输入中的关键信号词。',
  ].join('\n');

  const userPrompt = [
    `视频名: ${args.originalName}`,
    `AI简介: ${args.aiCaption || ''}`,
    `视频时长(秒): ${typeof args.durationSec === 'number' && args.durationSec > 0 ? args.durationSec : '未知'}`,
    '可选分类体系如下：',
    taxonomyText,
    '判定提醒：',
    forcedAdultHint
      ? `- 强规则：当前输入命中成人内容强信号，一级分类优先限定为【${forcedAdultHint.level1}】；若二级不确定，优先考虑【${forcedAdultHint.level2}】。`
      : '- 当前输入未命中成人内容强规则，可按常规视频分类。',
    '- 若内容是小品/春晚语言类节目（含赵本山等演员信号），一级优先用【综艺】。',
    '- 不要仅因“第X集”而判为【电视剧/剧集】。',
    '- 仅当输入明确出现“短剧/微短剧”等非成人短剧信号时才使用【短剧】。',
    '输出要求：level1 必须命中一级分类；level2 输出1-3个且必须属于对应 level1。',
  ].join('\n');

  let level1 = '';
  let level2: string[] = [];
  let confidence = 0;
  let reason = '';

  try {
    const raw = await generateTextWithAiProfile(args.profile, systemPrompt, userPrompt);
    logger.info('[typeb_category] AI原始分类结果', {
      mediaAssetId: args.mediaAssetId.toString(),
      raw,
    });

    const parsed = parseAiJson(raw);
    level1 = String(parsed.level1 || '').trim();
    level2 = Array.isArray(parsed.level2)
      ? parsed.level2.map((x) => String(x).trim()).filter(Boolean)
      : [];
    confidence = Number(parsed.confidence || 0);
    reason = String(parsed.reason || '');
  } catch (error) {
    logger.warn('[typeb_category] AI分类解析失败，跳过分类写入', {
      mediaAssetId: args.mediaAssetId.toString(),
      error: error instanceof Error ? error.message : String(error),
    });
    return { ok: false as const, reason: 'ai_parse_failed' as const };
  }

  if (forcedAdultHint && !ADULT_LEVEL1_NAMES.has(level1)) {
    logger.warn('[typeb_category] AI返回非成人一级分类，按强规则改写', {
      mediaAssetId: args.mediaAssetId.toString(),
      aiLevel1: level1,
      forcedLevel1: forcedAdultHint.level1,
      forcedLevel2: forcedAdultHint.level2,
      matchedKeywords: forcedAdultHint.matchedKeywords,
    });
    level1 = forcedAdultHint.level1;
    level2 = [forcedAdultHint.level2];
    confidence = Math.max(Number.isFinite(confidence) ? confidence : 0, forcedAdultHint.confidence);
    reason = `${forcedAdultHint.reason}；AI 原始结果已忽略。`;
  }

  if (!LEVEL1_NAMES.includes(level1)) {
    logger.warn('[typeb_category] 一级分类不合法，跳过写入', {
      mediaAssetId: args.mediaAssetId.toString(),
      level1,
      allowedLevel1: LEVEL1_NAMES,
    });
    return { ok: false as const, reason: 'invalid_level1' as const, level1 };
  }

  const allowedLevel2 = new Set(CATEGORY_MAP[level1]);
  let normalizedLevel2 = Array.from(new Set(level2.filter((name) => allowedLevel2.has(name)))).slice(0, 3);

  if (
    normalizedLevel2.length === 0 &&
    forcedAdultHint &&
    level1 === forcedAdultHint.level1 &&
    allowedLevel2.has(forcedAdultHint.level2)
  ) {
    normalizedLevel2 = [forcedAdultHint.level2];
    if (!reason) {
      reason = forcedAdultHint.reason;
    }
  }

  if (normalizedLevel2.length === 0) {
    logger.warn('[typeb_category] 二级分类为空，跳过写入', {
      mediaAssetId: args.mediaAssetId.toString(),
      level1,
      rawLevel2: level2,
    });
    return { ok: false as const, reason: 'empty_level2' as const, level1 };
  }

  const level1Id = await ensureLevel1(level1);

  logger.info('[typeb_category] 分类校验通过，开始写入关系表', {
    mediaAssetId: args.mediaAssetId.toString(),
    level1,
    level2: normalizedLevel2,
  });

  await prisma.$executeRawUnsafe(`DELETE FROM media_asset_categories WHERE media_asset_id = $1`, args.mediaAssetId);

  for (const level2Name of normalizedLevel2) {
    const level2Id = await ensureLevel2(level1Id, level2Name);
    await prisma.$executeRawUnsafe(
      `
      INSERT INTO media_asset_categories(media_asset_id, level2_id, created_at)
      VALUES ($1, $2, now())
      ON CONFLICT (media_asset_id, level2_id) DO NOTHING
    `,
      args.mediaAssetId,
      level2Id,
    );
  }

  logger.info('[typeb_category] 分类写入成功', {
    mediaAssetId: args.mediaAssetId.toString(),
    level1,
    level2: normalizedLevel2,
    confidence,
    reason,
  });

  return {
    ok: true as const,
    level1,
    level2: normalizedLevel2,
    confidence,
    reason,
  };
}

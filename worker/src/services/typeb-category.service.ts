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
};

const LEVEL1_NAMES = Object.keys(CATEGORY_MAP);

function slugify(input: string) {
  return input
    .toLowerCase()
    .replace(/\s+/g, '-')
    .replace(/[^\w\-\u4e00-\u9fa5]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}

function parseAiJson(text: string): { level1?: string; level2?: string[]; confidence?: number; reason?: string } {
  const trimmed = text.trim();
  const block = trimmed.match(/\{[\s\S]*\}/)?.[0] || trimmed;
  return JSON.parse(block);
}

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

export async function classifyAndAssignForTypeB(args: {
  mediaAssetId: bigint;
  originalName: string;
  aiCaption: string;
  profile: Pick<AiModelProfile, 'endpointUrl' | 'apiKeyEncrypted' | 'model' | 'temperature' | 'topP' | 'maxTokens'>;
}) {
  logger.info('[typeb_category] 开始自动生成分类', {
    mediaAssetId: args.mediaAssetId.toString(),
    originalName: args.originalName,
    hasCaption: Boolean(args.aiCaption),
  });
  const taxonomyText = LEVEL1_NAMES.map((l1) => `${l1}: ${CATEGORY_MAP[l1].join('、')}`).join('\n');

  const systemPrompt = [
    '你是视频分类器，只能基于输入信息在给定分类体系中选择。',
    '必须输出严格 JSON，禁止输出 Markdown、代码块、额外解释。',
    'JSON格式: {"level1":"...","level2":["..."],"confidence":0.0,"reason":"..."}',
    '一级分类判定规则（按优先级执行）：',
    '1) 若出现“小品/春晚小品/相声/赵本山/宋丹丹/范伟/小沈阳”等强信号，优先判定为【综艺】。',
    '2) 只有明确出现“连续剧/剧集/第X季/连载剧情”等长剧情信号时，才判定为【电视剧/剧集】。',
    '3) 只有明确出现“短剧/竖屏短剧/微短剧/男频女频短剧”等信号时，才判定为【短剧】。',
    '4) 不要因为“第X集”就默认判定为【电视剧/剧集】；合集分集的小品仍可属于【综艺】。',
    '5) level2 必须从对应 level1 的可选项中选择 1-3 个，若不确定优先选择语义最稳妥的单个标签（如“喜剧”或“生活”）。',
    '6) confidence 取值 0-1；reason 用一句中文简述依据，需引用输入中的关键信号词。',
  ].join('\n');

  const userPrompt = [
    `视频名: ${args.originalName}`,
    `AI简介: ${args.aiCaption || ''}`,
    '可选分类体系如下：',
    taxonomyText,
    '判定提醒：',
    '- 若内容是小品/春晚语言类节目（含赵本山等演员信号），一级优先用【综艺】。',
    '- 不要仅因“第X集”而判为【电视剧/剧集】。',
    '- 仅当输入明确出现“短剧/微短剧”等词时才使用【短剧】。',
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

  if (!LEVEL1_NAMES.includes(level1)) {
    logger.warn('[typeb_category] 一级分类不合法，跳过写入', {
      mediaAssetId: args.mediaAssetId.toString(),
      level1,
      allowedLevel1: LEVEL1_NAMES,
    });
    return { ok: false as const, reason: 'invalid_level1' as const, level1 };
  }

  const allowedLevel2 = new Set(CATEGORY_MAP[level1]);
  const normalizedLevel2 = Array.from(new Set(level2.filter((name) => allowedLevel2.has(name)))).slice(0, 3);

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

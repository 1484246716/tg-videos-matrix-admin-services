import { ContentTag } from '@prisma/client';

export const ADULT_CONTENT_TAG_SCOPE = 'adult_18';
export const DEFAULT_CONTENT_TAG_CANDIDATE_LIMIT = 20;
export const MAX_CONTENT_TAG_CANDIDATE_LIMIT = 30;

type AdultContentTagRecord = Pick<ContentTag, 'id' | 'name' | 'slug' | 'sort' | 'status' | 'scope'>;

type RecallSourceName = 'channel_default' | 'original_name' | 'source_caption' | 'source_channel' | 'ai_caption';

type RecallSource = {
  name: Exclude<RecallSourceName, 'channel_default'>;
  text: string;
  scoreWeight: number;
  strongSignal: boolean;
};

export type AdultContentTagCandidate = {
  tagId: bigint;
  name: string;
  slug: string;
  score: number;
  isStrong: boolean;
  matchedSources: RecallSourceName[];
  matchedKeywords: string[];
};

export type AdultContentTagRecallResult = {
  baseTagIds: bigint[];
  candidateTagIds: bigint[];
  ruleTagIds: bigint[];
  candidates: AdultContentTagCandidate[];
  sourceSnapshot: {
    originalName: string;
    aiCaption: string | null;
    sourceCaption: string | null;
    sourceChannel: string | null;
  };
};

const TAG_ALIAS_MAP: Record<string, string[]> = {
  '3P': ['3p', '三人', '多人'],
  'OK': ['ok'],
  'OL': ['ol', 'office lady', 'office-lady', '上班族'],
  'SM': ['sm', 'bdsm', 's/m'],
  '办公室女士': ['office lady', 'office-lady', '办公室女郎'],
  '办公室': ['office'],
  '教师': ['老师', 'teacher'],
  '女仆': ['maid'],
  '学生': ['student', '学生装'],
  '熟女': ['mature', 'mature woman'],
  '少妇': ['young married woman'],
  '人妻': ['married woman'],
  '素人': ['amateur'],
  '空姐': ['flight attendant', '空乘'],
  '业余': ['part time', 'part-time', ' amateur '],
  '角色扮演': ['roleplay', 'role play', 'cosplay'],
  '户外': ['outdoor'],
  '公共': ['public'],
  '健身': ['fitness', 'gym'],
  '剧情': ['story', 'storyline'],
  '偷情': ['cheating'],
  '调教': ['training', 'bdsm'],
  '捆绑': ['bondage', '束缚'],
  '虐恋': ['bdsm romance'],
  '巨乳': ['大胸', 'large breasts', 'large-breasts'],
  '大奶': ['big boobs', 'big-boobs', '爆乳'],
  '乳交': ['paizuri', '胸交'],
  '大屁股': ['big butt', 'big-butt', '翘臀'],
  '美臀': ['nice butt', 'nice-butt', '翘臀'],
  '美腿控': ['leg fetish', 'leg-fetish', '美腿'],
  '喷水': ['squirting'],
  '口交': ['oral', 'oral sex', 'oral-sex', 'blowjob'],
  '肛交': ['anal', 'anal sex', 'anal-sex'],
  '拳交': ['fisting'],
  '群P': ['group sex', 'group-sex', '群交', '多人'],
  '射精': ['ejaculation'],
  '内射': ['internal ejaculation', 'internal-ejaculation', '中出'],
  '无套内射': ['creampie', 'finish inside without condom', 'finish-inside-without-condom', '无套', '无套中出'],
  '无套中出': ['finish inside without condom', 'finish-inside-without-condom', '无套', '无套内射'],
  '中出': ['finish inside', 'finish-inside', '内射'],
  '颜射': ['facial'],
  '自慰': ['masturbation', '手淫'],
  '精液自慰': ['semen masturbation', 'semen-masturbation-technique'],
  '制服': ['uniform', '制服诱惑'],
  '内衣': ['underwear', '情趣内衣'],
  '丝袜': ['stockings', '丝袜美腿'],
  '黑丝': ['black stockings', 'black-stockings', '黑丝袜'],
  '白丝': ['white stockings', 'white-stockings', '白丝袜'],
  '高跟鞋': ['high heels', 'high-heels', '高跟'],
  '道具': ['props'],
};

export function buildAdultContentTagRecall(args: {
  activeTags: AdultContentTagRecord[];
  channelDefaultTags: Array<Pick<ContentTag, 'id' | 'name' | 'slug'>>;
  originalName: string;
  aiCaption?: string | null;
  sourceMeta: unknown;
  candidateLimit?: number;
}): AdultContentTagRecallResult {
  const candidateLimit = normalizeCandidateLimit(args.candidateLimit);
  const sourceMeta = getSourceMetaObject(args.sourceMeta);
  const sourceCaption = pickFirstNonEmptyString(sourceMeta, ['messageTxt', 'txtContent', 'messageText', 'caption']);
  const sourceChannel = pickFirstNonEmptyString(sourceMeta, [
    'sourceChannelUsername',
    'cloneSourceChannelUsername',
    'sourceChannelTitle',
    'sourceChannelName',
  ]);

  const recallSources = buildRecallSources({
    originalName: args.originalName,
    aiCaption: args.aiCaption ?? null,
    sourceCaption,
    sourceChannel,
  });

  const candidateMap = new Map<string, {
    tag: AdultContentTagRecord;
    score: number;
    isStrong: boolean;
    matchedSources: Set<RecallSourceName>;
    matchedKeywords: Set<string>;
  }>();

  for (const defaultTag of args.channelDefaultTags) {
    const key = defaultTag.id.toString();
    const existing = candidateMap.get(key);
    if (existing) {
      existing.score += 1000;
      existing.isStrong = true;
      existing.matchedSources.add('channel_default');
      existing.matchedKeywords.add(defaultTag.name);
      continue;
    }

    const fallbackTag = args.activeTags.find((tag) => tag.id === defaultTag.id);
    if (!fallbackTag) continue;

    candidateMap.set(key, {
      tag: fallbackTag,
      score: 1000,
      isStrong: true,
      matchedSources: new Set<RecallSourceName>(['channel_default']),
      matchedKeywords: new Set<string>([defaultTag.name]),
    });
  }

  for (const tag of args.activeTags) {
    const keywords = buildTagKeywords(tag);
    for (const source of recallSources) {
      const matchedKeyword = keywords.find((keyword) => containsKeyword(source.text, keyword));
      if (!matchedKeyword) continue;

      const key = tag.id.toString();
      const existing = candidateMap.get(key);
      const scoreBoost = source.scoreWeight + Math.min(normalizeKeyword(matchedKeyword).length, 18);

      if (existing) {
        existing.score += scoreBoost;
        existing.isStrong = existing.isStrong || source.strongSignal;
        existing.matchedSources.add(source.name);
        existing.matchedKeywords.add(matchedKeyword);
      } else {
        candidateMap.set(key, {
          tag,
          score: scoreBoost,
          isStrong: source.strongSignal,
          matchedSources: new Set<RecallSourceName>([source.name]),
          matchedKeywords: new Set<string>([matchedKeyword]),
        });
      }
    }
  }

  const candidates = Array.from(candidateMap.values())
    .sort((left, right) => {
      if (right.score !== left.score) return right.score - left.score;
      if (left.tag.sort !== right.tag.sort) return left.tag.sort - right.tag.sort;
      return left.tag.name.localeCompare(right.tag.name, 'zh-CN');
    })
    .slice(0, candidateLimit)
    .map<AdultContentTagCandidate>((entry) => ({
      tagId: entry.tag.id,
      name: entry.tag.name,
      slug: entry.tag.slug,
      score: entry.score,
      isStrong: entry.isStrong,
      matchedSources: Array.from(entry.matchedSources.values()),
      matchedKeywords: Array.from(entry.matchedKeywords.values()),
    }));

  const baseTagIds = dedupeBigInt([
    ...args.channelDefaultTags.map((tag) => tag.id),
    ...candidates.filter((candidate) => candidate.isStrong).map((candidate) => candidate.tagId),
  ]);

  return {
    baseTagIds,
    candidateTagIds: dedupeBigInt(candidates.map((candidate) => candidate.tagId)),
    ruleTagIds: dedupeBigInt(candidates.map((candidate) => candidate.tagId)),
    candidates,
    sourceSnapshot: {
      originalName: args.originalName,
      aiCaption: normalizeOptionalText(args.aiCaption ?? null),
      sourceCaption: normalizeOptionalText(sourceCaption),
      sourceChannel: normalizeOptionalText(sourceChannel),
    },
  };
}

function buildRecallSources(args: {
  originalName: string;
  aiCaption: string | null;
  sourceCaption: string | null;
  sourceChannel: string | null;
}): RecallSource[] {
  const result: RecallSource[] = [];
  const pushIfPresent = (source: RecallSource) => {
    const text = normalizeOptionalText(source.text);
    if (!text) return;
    result.push({ ...source, text });
  };

  pushIfPresent({
    name: 'original_name',
    text: args.originalName,
    scoreWeight: 120,
    strongSignal: true,
  });
  pushIfPresent({
    name: 'source_caption',
    text: args.sourceCaption ?? '',
    scoreWeight: 90,
    strongSignal: true,
  });
  pushIfPresent({
    name: 'source_channel',
    text: args.sourceChannel ?? '',
    scoreWeight: 100,
    strongSignal: true,
  });
  pushIfPresent({
    name: 'ai_caption',
    text: args.aiCaption ?? '',
    scoreWeight: 50,
    strongSignal: false,
  });

  return result;
}

function buildTagKeywords(tag: AdultContentTagRecord) {
  const keywords = new Set<string>();
  addKeyword(keywords, tag.name);
  addKeyword(keywords, tag.slug);
  addKeyword(keywords, tag.slug.replace(/^tag-/, ''));
  addKeyword(keywords, tag.slug.replace(/-/g, ' '));
  addKeyword(keywords, tag.slug.replace(/-/g, ''));

  for (const alias of TAG_ALIAS_MAP[tag.name] ?? []) {
    addKeyword(keywords, alias);
  }

  for (const alias of TAG_ALIAS_MAP[tag.slug] ?? []) {
    addKeyword(keywords, alias);
  }

  return Array.from(keywords.values());
}

function addKeyword(target: Set<string>, value: string | null | undefined) {
  const normalized = normalizeOptionalText(value);
  if (!normalized) return;
  target.add(normalized);
}

function containsKeyword(text: string, keyword: string) {
  const normalizedText = normalizeKeyword(text);
  const normalizedKeyword = normalizeKeyword(keyword);
  if (!normalizedText || !normalizedKeyword) return false;

  if (/^[a-z0-9]+$/i.test(normalizedKeyword) && normalizedKeyword.length <= 3) {
    return new RegExp(`(^|[^a-z0-9])${escapeRegExp(normalizedKeyword)}([^a-z0-9]|$)`, 'i').test(
      normalizeBoundaryText(text),
    );
  }

  return normalizedText.includes(normalizedKeyword);
}

function normalizeBoundaryText(value: string) {
  return String(value || '')
    .toLowerCase()
    .replace(/[_/\\.-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeKeyword(value: string) {
  return String(value || '')
    .toLowerCase()
    .replace(/[_/\\.\-\s]+/g, '')
    .trim();
}

function normalizeOptionalText(value: string | null | undefined) {
  const normalized = String(value || '').trim();
  return normalized ? normalized : null;
}

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function pickFirstNonEmptyString(source: Record<string, unknown> | null, keys: string[]) {
  if (!source) return null;
  for (const key of keys) {
    const value = source[key];
    if (typeof value === 'string' && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

function getSourceMetaObject(sourceMeta: unknown) {
  return sourceMeta && typeof sourceMeta === 'object' && !Array.isArray(sourceMeta)
    ? (sourceMeta as Record<string, unknown>)
    : null;
}

function normalizeCandidateLimit(value: number | null | undefined) {
  const numeric = typeof value === 'number' && Number.isFinite(value) ? Math.floor(value) : DEFAULT_CONTENT_TAG_CANDIDATE_LIMIT;
  return Math.min(MAX_CONTENT_TAG_CANDIDATE_LIMIT, Math.max(1, numeric));
}

function dedupeBigInt(values: bigint[]) {
  return Array.from(new Set(values.map((value) => value.toString()))).map((value) => BigInt(value));
}

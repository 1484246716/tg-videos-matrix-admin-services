import {
  ORDER_STRATEGY_FEATURE_ENABLED,
  ORDER_STRATEGY_HEAD_BYPASS_ENABLED,
  ORDER_STRATEGY_NORMAL_DISPATCH_GATE_ENABLED,
  ORDER_STRATEGY_NORMAL_UPLOAD_GATE_ENABLED,
} from '../config/env';

export type OrderType = 'normal' | 'collection';

export type ResolvedOrderMeta = {
  orderType: OrderType;
  orderGroup: string;
  orderNo: number | null;
  orderParseFailed: boolean;
  collectionName: string | null;
  episodeNo: number | null;
  isCollection: boolean;
};

export type OrderConfig = {
  orderGateEnabled: boolean;
  normalOrderUploadGateEnabled: boolean;
  normalOrderDispatchGateEnabled: boolean;
  orderHeadBypassEnabled: boolean;
  orderHeadBypassMinutes: number;
  normalOrderKeyMode: 'mtime_file_path' | 'created_at_id';
  episodeRuleMode: 'legacy' | 'profiled';
  episodeNoPatternEnabled: boolean;
  episodeChineseNumberEnabled: boolean;
  episodeAliases: string[];
  episodePrefixTokens: string[];
  episodeSuffixTokens: string[];
};

export type CollectionGapPolicy = 'strict' | 'allow_gap';

export type CollectionOrderConfig = {
  inheritChannelOrderConfig: boolean;
  collectionDispatchGateEnabled: boolean;
  collectionHeadBypassEnabled: boolean;
  collectionHeadBypassMinutes: number;
  collectionGapPolicy: CollectionGapPolicy;
  collectionAllowedGapSize: number;
};

export type ResolvedCollectionDispatchOrderConfig = {
  inheritChannelOrderConfig: boolean;
  orderGateEnabled: boolean;
  orderHeadBypassEnabled: boolean;
  orderHeadBypassMinutes: number;
  collectionGapPolicy: CollectionGapPolicy;
  collectionAllowedGapSize: number;
};

export type EpisodeParseResult = {
  episodeNo: number | null;
  orderNo: number | null;
  orderParseFailed: boolean;
  matchedBy:
    | 'prefix_token'
    | 'alias_pattern'
    | 'suffix_token'
    | 'season_episode'
    | 'plain_number'
    | 'legacy_pattern'
    | 'unmatched';
  matchedToken: string | null;
};

const DEFAULT_EPISODE_ALIASES = [
  '集',
  '期',
  '话',
  '季',
  '部',
  '弹',
  '场',
  '出',
  '播',
  '章',
  '节',
  '回',
  '卷',
  '册',
  '篇',
  '版',
  '辑',
  '编',
  '讲',
  '课',
  '首',
  '曲',
  '关',
  '局',
  '赛季',
  '轮',
  '届',
  '次',
  '序言',
  '序章',
  '前言',
  '引言',
  '楔子',
  '前传',
  '先导片',
  '预告片',
  '季前赛',
  'Ova版',
  '番外',
  '番外篇',
  '特别篇',
  'SP',
  'OVA',
  'OAD',
  '剧场版',
  '特典',
  '幕后花絮',
  '资料片',
  'DLC',
  '总集篇',
  '终章',
  '最终话',
  '最终回',
  '最终局',
  '尾声',
  '后记',
  '大结局',
  '完结篇',
  '后传',
  '总决赛',
] as const;

const DEFAULT_EPISODE_PREFIX_TOKENS = [
  '序言',
  '序章',
  '前言',
  '引言',
  '楔子',
  '前传',
  '先导片',
  '预告片',
  '季前赛',
  'Ova版',
  '番外',
  '番外篇',
  '特别篇',
  'SP',
  'OVA',
  'OAD',
  '剧场版',
  '特典',
] as const;

const DEFAULT_EPISODE_SUFFIX_TOKENS = [
  '幕后花絮',
  '资料片',
  'DLC',
  '总集篇',
  '终章',
  '最终话',
  '最终回',
  '最终局',
  '尾声',
  '后记',
  '大结局',
  '完结篇',
  '后传',
  '总决赛',
] as const;

const EPISODE_SPECIAL_ORDER_BASE = 100000;
const EPISODE_SPECIAL_TOKEN_BUCKET = 1000;

export const DEFAULT_ORDER_CONFIG: OrderConfig = {
  orderGateEnabled: true,
  normalOrderUploadGateEnabled: true,
  normalOrderDispatchGateEnabled: true,
  orderHeadBypassEnabled: false,
  orderHeadBypassMinutes: 180,
  normalOrderKeyMode: 'mtime_file_path',
  episodeRuleMode: 'profiled',
  episodeNoPatternEnabled: true,
  episodeChineseNumberEnabled: true,
  episodeAliases: [...DEFAULT_EPISODE_ALIASES],
  episodePrefixTokens: [...DEFAULT_EPISODE_PREFIX_TOKENS],
  episodeSuffixTokens: [...DEFAULT_EPISODE_SUFFIX_TOKENS],
};

function asObject(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, unknown>;
}

function normalizeStringList(value: unknown, fallback: readonly string[]) {
  if (!Array.isArray(value)) {
    return [...fallback];
  }

  const seen = new Set<string>();
  const normalized: string[] = [];
  for (const item of value) {
    if (typeof item !== 'string') continue;
    const trimmed = item.trim();
    if (!trimmed) continue;
    const key = trimmed.toLocaleLowerCase('zh-CN');
    if (seen.has(key)) continue;
    seen.add(key);
    normalized.push(trimmed);
  }

  return normalized.length > 0 ? normalized : [...fallback];
}

function parseIntegerOrderNo(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value) && value !== 0) {
    return Math.trunc(value);
  }
  if (typeof value === 'string' && /^-?\d+$/.test(value.trim())) {
    const parsed = Number(value.trim());
    if (Number.isFinite(parsed) && parsed !== 0) {
      return Math.trunc(parsed);
    }
  }
  return null;
}

export function parsePositiveOrderNo(value: unknown): number | null {
  const parsed = parseIntegerOrderNo(value);
  return parsed !== null && parsed > 0 ? parsed : null;
}

export function buildNormalOrderGroup(channelId: bigint | string | number) {
  return `normal:${channelId.toString()}`;
}

export function normalizeCollectionOrderGroupName(collectionName: string) {
  return collectionName.trim().toLocaleLowerCase('zh-CN');
}

export function buildCollectionOrderGroup(
  channelId: bigint | string | number,
  collectionName: string,
) {
  return `collection:${channelId.toString()}:${normalizeCollectionOrderGroupName(collectionName)}`;
}

export function buildNormalOrderMeta(args: {
  channelId: bigint | string | number;
  orderNo: number;
}) {
  return {
    orderType: 'normal' as const,
    orderGroup: buildNormalOrderGroup(args.channelId),
    orderNo: Math.floor(args.orderNo),
    orderParseFailed: false,
  };
}

export function buildCollectionOrderMeta(args: {
  channelId: bigint | string | number;
  collectionName: string;
  episodeNo: number | null;
  episodeParseFailed: boolean;
  orderNo?: number | null;
}) {
  const orderNo =
    parseIntegerOrderNo(args.orderNo) ??
    (args.episodeNo !== null ? Math.floor(args.episodeNo) : null);
  return {
    orderType: 'collection' as const,
    orderGroup: buildCollectionOrderGroup(args.channelId, args.collectionName),
    orderNo,
    orderParseFailed: args.episodeParseFailed || orderNo === null,
  };
}

function escapeRegExp(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function parseChineseNumber(value: string) {
  const normalized = value.replace(/两/g, '二').replace(/〇/g, '零').trim();
  if (!normalized) return null;

  const digitMap: Record<string, number> = {
    零: 0,
    一: 1,
    二: 2,
    三: 3,
    四: 4,
    五: 5,
    六: 6,
    七: 7,
    八: 8,
    九: 9,
  };
  const unitMap: Record<string, number> = {
    十: 10,
    百: 100,
    千: 1000,
    万: 10000,
  };

  if (/^[零一二三四五六七八九]+$/.test(normalized)) {
    const digits = normalized
      .split('')
      .map((char) => digitMap[char])
      .filter((digit) => digit !== undefined);
    if (digits.length !== normalized.length) return null;
    return Number(digits.join(''));
  }

  let total = 0;
  let section = 0;
  let number = 0;
  for (const char of normalized) {
    if (digitMap[char] !== undefined) {
      number = digitMap[char];
      continue;
    }

    const unit = unitMap[char];
    if (!unit) return null;

    if (unit === 10000) {
      section = (section + (number || 0)) * unit;
      total += section;
      section = 0;
      number = 0;
      continue;
    }

    section += (number || 1) * unit;
    number = 0;
  }

  const parsed = total + section + number;
  return parsed > 0 ? parsed : null;
}

function parseEpisodeNumberToken(value: string, allowChineseNumber: boolean) {
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (/^\d+$/.test(trimmed)) {
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : null;
  }
  if (!allowChineseNumber) return null;
  return parseChineseNumber(trimmed);
}

function extractEpisodeNumberAroundToken(args: {
  text: string;
  token: string;
  allowChineseNumber: boolean;
}) {
  const escapedToken = escapeRegExp(args.token);
  const numberPattern = '(\\d+|[零〇一二三四五六七八九十百千万两]+)';
  const patterns = [
    new RegExp(`${escapedToken}\\s*${numberPattern}`, 'i'),
    new RegExp(`${escapedToken}[\\s_\\-:：]*第?\\s*${numberPattern}`, 'i'),
    new RegExp(`第\\s*${numberPattern}\\s*${escapedToken}`, 'i'),
    new RegExp(`(^|[^A-Za-z0-9])${numberPattern}\\s*${escapedToken}`, 'i'),
  ];

  for (const pattern of patterns) {
    const match = args.text.match(pattern);
    const rawValue = match?.[1] && match[1].trim() ? match[1] : match?.[2];
    if (!rawValue) continue;
    const parsed = parseEpisodeNumberToken(rawValue, args.allowChineseNumber);
    if (parsed !== null) {
      return parsed;
    }
  }

  return null;
}

function buildSpecialEpisodeOrderNo(args: {
  tokenIndex: number;
  numericOrder: number | null;
  kind: 'prefix' | 'suffix';
}) {
  const safeIndex = Math.max(0, Math.floor(args.tokenIndex));
  const safeNumeric = args.numericOrder !== null ? Math.min(args.numericOrder, 999) : 0;
  const base = EPISODE_SPECIAL_ORDER_BASE + safeIndex * EPISODE_SPECIAL_TOKEN_BUCKET + safeNumeric;
  return args.kind === 'prefix' ? -base : base;
}

function parseLegacyEpisodeOrder(text: string): EpisodeParseResult {
  const patterns = [
    /\[第\s*(\d+)\s*(?:集|话|話)\]/,
    /第\s*(\d+)\s*(?:集|话|話)/,
    /S\d+E(\d+)/i,
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match || !match[1]) continue;
    const parsed = Number(match[1]);
    if (!Number.isFinite(parsed) || parsed <= 0) continue;
    return {
      episodeNo: Math.floor(parsed),
      orderNo: Math.floor(parsed),
      orderParseFailed: false,
      matchedBy: 'legacy_pattern',
      matchedToken: null,
    };
  }

  return {
    episodeNo: null,
    orderNo: null,
    orderParseFailed: true,
    matchedBy: 'unmatched',
    matchedToken: null,
  };
}

export function parseEpisodeOrderFromText(text: string, config?: Partial<OrderConfig>): EpisodeParseResult {
  const safeText = text.trim();
  if (!safeText) {
    return {
      episodeNo: null,
      orderNo: null,
      orderParseFailed: true,
      matchedBy: 'unmatched',
      matchedToken: null,
    };
  }

  const mergedConfig: OrderConfig = {
    ...DEFAULT_ORDER_CONFIG,
    ...config,
    episodeAliases: normalizeStringList(
      config?.episodeAliases,
      DEFAULT_ORDER_CONFIG.episodeAliases,
    ),
    episodePrefixTokens: normalizeStringList(
      config?.episodePrefixTokens,
      DEFAULT_ORDER_CONFIG.episodePrefixTokens,
    ),
    episodeSuffixTokens: normalizeStringList(
      config?.episodeSuffixTokens,
      DEFAULT_ORDER_CONFIG.episodeSuffixTokens,
    ),
  };

  if (mergedConfig.episodeRuleMode === 'legacy') {
    return parseLegacyEpisodeOrder(safeText);
  }

  const prefixTokens = mergedConfig.episodePrefixTokens;
  for (let index = 0; index < prefixTokens.length; index += 1) {
    const token = prefixTokens[index];
    if (!safeText.toLocaleLowerCase('zh-CN').includes(token.toLocaleLowerCase('zh-CN'))) {
      continue;
    }
    const episodeNo = extractEpisodeNumberAroundToken({
      text: safeText,
      token,
      allowChineseNumber: mergedConfig.episodeChineseNumberEnabled,
    });
    return {
      episodeNo,
      orderNo: buildSpecialEpisodeOrderNo({
        tokenIndex: index,
        numericOrder: episodeNo,
        kind: 'prefix',
      }),
      orderParseFailed: false,
      matchedBy: 'prefix_token',
      matchedToken: token,
    };
  }

  if (mergedConfig.episodeNoPatternEnabled && mergedConfig.episodeAliases.length > 0) {
    const aliasPattern = mergedConfig.episodeAliases
      .slice()
      .sort((left, right) => right.length - left.length)
      .map((token) => escapeRegExp(token))
      .join('|');
    const patterns = [
      new RegExp(`第\\s*(\\d+|[零〇一二三四五六七八九十百千万两]+)\\s*(?:${aliasPattern})`, 'i'),
      new RegExp(`(^|[^A-Za-z0-9])(\\d+|[零〇一二三四五六七八九十百千万两]+)\\s*(?:${aliasPattern})`, 'i'),
    ];

    for (const pattern of patterns) {
      const match = safeText.match(pattern);
      const rawValue = match?.[1] && match[1].trim() ? match[1] : match?.[2];
      if (!rawValue) continue;
      const episodeNo = parseEpisodeNumberToken(
        rawValue,
        mergedConfig.episodeChineseNumberEnabled,
      );
      if (episodeNo === null) continue;
      return {
        episodeNo,
        orderNo: episodeNo,
        orderParseFailed: false,
        matchedBy: 'alias_pattern',
        matchedToken: null,
      };
    }
  }

  const suffixTokens = mergedConfig.episodeSuffixTokens;
  for (let index = 0; index < suffixTokens.length; index += 1) {
    const token = suffixTokens[index];
    if (!safeText.toLocaleLowerCase('zh-CN').includes(token.toLocaleLowerCase('zh-CN'))) {
      continue;
    }
    const episodeNo = extractEpisodeNumberAroundToken({
      text: safeText,
      token,
      allowChineseNumber: mergedConfig.episodeChineseNumberEnabled,
    });
    return {
      episodeNo,
      orderNo: buildSpecialEpisodeOrderNo({
        tokenIndex: index,
        numericOrder: episodeNo,
        kind: 'suffix',
      }),
      orderParseFailed: false,
      matchedBy: 'suffix_token',
      matchedToken: token,
    };
  }

  const seasonEpisodeMatch = safeText.match(/S\d+E(\d+)/i);
  if (seasonEpisodeMatch?.[1]) {
    const episodeNo = Number(seasonEpisodeMatch[1]);
    if (Number.isFinite(episodeNo) && episodeNo > 0) {
      return {
        episodeNo: Math.floor(episodeNo),
        orderNo: Math.floor(episodeNo),
        orderParseFailed: false,
        matchedBy: 'season_episode',
        matchedToken: null,
      };
    }
  }

  const plainNumbers = [...safeText.matchAll(/(?:^|[^\d])(\d{1,3})(?!\d)/g)];
  for (let index = plainNumbers.length - 1; index >= 0; index -= 1) {
    const rawValue = plainNumbers[index]?.[1];
    if (!rawValue) continue;
    const episodeNo = Number(rawValue);
    if (!Number.isFinite(episodeNo) || episodeNo <= 0) continue;
    return {
      episodeNo: Math.floor(episodeNo),
      orderNo: Math.floor(episodeNo),
      orderParseFailed: false,
      matchedBy: 'plain_number',
      matchedToken: null,
    };
  }

  return {
    episodeNo: null,
    orderNo: null,
    orderParseFailed: true,
    matchedBy: 'unmatched',
    matchedToken: null,
  };
}

export function resolveOrderMeta(args: {
  channelId: bigint | string | number;
  sourceMeta: unknown;
}): ResolvedOrderMeta {
  const meta = asObject(args.sourceMeta);
  const collectionName =
    typeof meta.collectionName === 'string' && meta.collectionName.trim()
      ? meta.collectionName.trim()
      : null;
  const episodeNo = parsePositiveOrderNo(meta.episodeNo);
  const isCollection = meta.isCollection === true || meta.orderType === 'collection' || Boolean(collectionName);
  const orderType: OrderType = isCollection ? 'collection' : 'normal';
  const derivedOrderGroup = isCollection && collectionName
    ? buildCollectionOrderGroup(args.channelId, collectionName)
    : buildNormalOrderGroup(args.channelId);
  const orderGroup =
    typeof meta.orderGroup === 'string' && meta.orderGroup.trim()
      ? meta.orderGroup.trim()
      : derivedOrderGroup;
  const orderNo = parseIntegerOrderNo(meta.orderNo) ?? (isCollection ? episodeNo : null);
  const orderParseFailed = typeof meta.orderParseFailed === 'boolean'
    ? meta.orderParseFailed
    : isCollection
      ? meta.episodeParseFailed === true || orderNo === null
      : false;

  return {
    orderType,
    orderGroup,
    orderNo,
    orderParseFailed,
    collectionName,
    episodeNo,
    isCollection,
  };
}

export function parseOrderSchedulerConfig(navReplyMarkup: unknown): OrderConfig {
  const root = asObject(navReplyMarkup);
  const cfg = asObject(root.__orderConfig);
  const fallback = DEFAULT_ORDER_CONFIG;

  return {
    orderGateEnabled:
      (typeof cfg.orderGateEnabled === 'boolean'
        ? cfg.orderGateEnabled
        : fallback.orderGateEnabled) && ORDER_STRATEGY_FEATURE_ENABLED,
    normalOrderUploadGateEnabled:
      (typeof cfg.normalOrderUploadGateEnabled === 'boolean'
        ? cfg.normalOrderUploadGateEnabled
        : fallback.normalOrderUploadGateEnabled) &&
      ORDER_STRATEGY_FEATURE_ENABLED &&
      ORDER_STRATEGY_NORMAL_UPLOAD_GATE_ENABLED,
    normalOrderDispatchGateEnabled:
      (typeof cfg.normalOrderDispatchGateEnabled === 'boolean'
        ? cfg.normalOrderDispatchGateEnabled
        : fallback.normalOrderDispatchGateEnabled) &&
      ORDER_STRATEGY_FEATURE_ENABLED &&
      ORDER_STRATEGY_NORMAL_DISPATCH_GATE_ENABLED,
    orderHeadBypassEnabled:
      (typeof cfg.orderHeadBypassEnabled === 'boolean'
        ? cfg.orderHeadBypassEnabled
        : fallback.orderHeadBypassEnabled) &&
      ORDER_STRATEGY_FEATURE_ENABLED &&
      ORDER_STRATEGY_HEAD_BYPASS_ENABLED,
    orderHeadBypassMinutes:
      typeof cfg.orderHeadBypassMinutes === 'number' && cfg.orderHeadBypassMinutes > 0
        ? Math.floor(cfg.orderHeadBypassMinutes)
        : fallback.orderHeadBypassMinutes,
    normalOrderKeyMode:
      cfg.normalOrderKeyMode === 'created_at_id' ? 'created_at_id' : fallback.normalOrderKeyMode,
    episodeRuleMode:
      cfg.episodeRuleMode === 'legacy' || cfg.episodeRuleMode === 'profiled'
        ? cfg.episodeRuleMode
        : fallback.episodeRuleMode,
    episodeNoPatternEnabled:
      typeof cfg.episodeNoPatternEnabled === 'boolean'
        ? cfg.episodeNoPatternEnabled
        : fallback.episodeNoPatternEnabled,
    episodeChineseNumberEnabled:
      typeof cfg.episodeChineseNumberEnabled === 'boolean'
        ? cfg.episodeChineseNumberEnabled
        : fallback.episodeChineseNumberEnabled,
    episodeAliases: normalizeStringList(cfg.episodeAliases, fallback.episodeAliases),
    episodePrefixTokens: normalizeStringList(
      cfg.episodePrefixTokens,
      fallback.episodePrefixTokens,
    ),
    episodeSuffixTokens: normalizeStringList(
      cfg.episodeSuffixTokens,
      fallback.episodeSuffixTokens,
    ),
  };
}

export function parseCollectionOrderConfig(extConfig: unknown): CollectionOrderConfig {
  const fallback: CollectionOrderConfig = {
    inheritChannelOrderConfig: true,
    collectionDispatchGateEnabled: true,
    collectionHeadBypassEnabled: false,
    collectionHeadBypassMinutes: 180,
    collectionGapPolicy: 'strict',
    collectionAllowedGapSize: 0,
  };

  const root = asObject(extConfig);
  const cfg = asObject(root.order);

  return {
    inheritChannelOrderConfig:
      typeof cfg.inheritChannelOrderConfig === 'boolean'
        ? cfg.inheritChannelOrderConfig
        : fallback.inheritChannelOrderConfig,
    collectionDispatchGateEnabled:
      typeof cfg.collectionDispatchGateEnabled === 'boolean'
        ? cfg.collectionDispatchGateEnabled
        : fallback.collectionDispatchGateEnabled,
    collectionHeadBypassEnabled:
      typeof cfg.collectionHeadBypassEnabled === 'boolean'
        ? cfg.collectionHeadBypassEnabled
        : fallback.collectionHeadBypassEnabled,
    collectionHeadBypassMinutes:
      typeof cfg.collectionHeadBypassMinutes === 'number' && cfg.collectionHeadBypassMinutes > 0
        ? Math.floor(cfg.collectionHeadBypassMinutes)
        : fallback.collectionHeadBypassMinutes,
    collectionGapPolicy:
      cfg.collectionGapPolicy === 'allow_gap' || cfg.collectionGapPolicy === 'strict'
        ? cfg.collectionGapPolicy
        : fallback.collectionGapPolicy,
    collectionAllowedGapSize:
      typeof cfg.collectionAllowedGapSize === 'number' && cfg.collectionAllowedGapSize >= 0
        ? Math.floor(cfg.collectionAllowedGapSize)
        : fallback.collectionAllowedGapSize,
  };
}

export function resolveCollectionDispatchOrderConfig(args: {
  channelConfig: OrderConfig;
  extConfig: unknown;
}): ResolvedCollectionDispatchOrderConfig {
  const collectionConfig = parseCollectionOrderConfig(args.extConfig);

  if (collectionConfig.inheritChannelOrderConfig) {
    return {
      inheritChannelOrderConfig: true,
      orderGateEnabled: args.channelConfig.orderGateEnabled,
      orderHeadBypassEnabled: args.channelConfig.orderHeadBypassEnabled,
      orderHeadBypassMinutes: args.channelConfig.orderHeadBypassMinutes,
      collectionGapPolicy: collectionConfig.collectionGapPolicy,
      collectionAllowedGapSize: collectionConfig.collectionAllowedGapSize,
    };
  }

  return {
    inheritChannelOrderConfig: false,
    orderGateEnabled: collectionConfig.collectionDispatchGateEnabled,
    orderHeadBypassEnabled: collectionConfig.collectionHeadBypassEnabled,
    orderHeadBypassMinutes: collectionConfig.collectionHeadBypassMinutes,
    collectionGapPolicy: collectionConfig.collectionGapPolicy,
    collectionAllowedGapSize: collectionConfig.collectionAllowedGapSize,
  };
}

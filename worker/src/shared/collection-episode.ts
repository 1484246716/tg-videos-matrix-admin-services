const COLLECTION_EPISODE_SUFFIXES = [
  '集',
  '期',
  '回',
  '季',
  '话',
  '話',
  '部',
  '幕',
  '场',
  '場',
  '折',
  '出',
  '番',
  '弹',
  '彈',
  '辑',
  '輯',
  '篇',
  '章',
  '节',
  '節',
  '段',
  '卷',
  '版',
  '轮',
  '輪',
  '档',
  '檔',
  '战',
  '戰',
  '案',
  '宗',
  '纪',
  '紀',
  '传',
  '傳',
  '录',
  '錄',
  '志',
  '记',
  '記',
  '史',
  '赋',
  '賦',
  '歌',
  '曲',
  '局',
  '盘',
  '盤',
  '赛',
  '賽',
  '役',
  '阶',
  '階',
  '级',
  '級',
  '阵',
  '陣',
  '波',
  '次',
  '序',
  '楔',
  '结',
  '結',
  '尾',
  '景',
  '帧',
  '幀',
  '轨',
  '軌',
  '册',
  '冊',
];

const COLLECTION_EPISODE_SUFFIX_PATTERN = COLLECTION_EPISODE_SUFFIXES.join('|');

const COLLECTION_EPISODE_PATTERNS = [
  new RegExp(`\\[第\\s*(\\d+)\\s*(?:${COLLECTION_EPISODE_SUFFIX_PATTERN})\\]`),
  new RegExp(`第\\s*(\\d+)\\s*(?:${COLLECTION_EPISODE_SUFFIX_PATTERN})`),
  /S\d+E(\d+)/i,
];

export function parseEpisodeNoFromText(text: string) {
  for (const pattern of COLLECTION_EPISODE_PATTERNS) {
    const match = text.match(pattern);
    if (!match || !match[1]) continue;
    const parsed = Number(match[1]);
    if (!Number.isFinite(parsed) || parsed <= 0) continue;
    return parsed;
  }
  return null;
}

export function stripEpisodePrefixForTemplate(title: string, episodeNo: number) {
  const escapedEpisodeNo = String(episodeNo).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const punctClass = '[:：—－.。、，,\\-]*';

  const patterns = [
    new RegExp(`^第\\s*0*${escapedEpisodeNo}\\s*(?:${COLLECTION_EPISODE_SUFFIX_PATTERN})\\s*${punctClass}\\s*`, 'u'),
    new RegExp(`^0*${escapedEpisodeNo}\\s*(?:${COLLECTION_EPISODE_SUFFIX_PATTERN})\\s*${punctClass}\\s*`, 'u'),
  ];

  let next = title.trim();
  for (const pattern of patterns) {
    next = next.replace(pattern, '').trim();
  }

  return next || title.trim();
}

export function formatCollectionEpisodeTitle(args: {
  collectionName?: string | null;
  episodeNo: number;
  episodeTitle?: string | null;
  sourceTitle?: string | null;
  templateText?: string | null;
}) {
  const safeEpisodeTitle = (args.episodeTitle || '').trim();
  if (safeEpisodeTitle) return safeEpisodeTitle;

  const normalizedCollectionName = (args.collectionName || '').trim();
  const fallbackTitle =
    (args.sourceTitle || '').trim() ||
    `${normalizedCollectionName}第${args.episodeNo}集`.trim() ||
    `第${args.episodeNo}集`;
  const safeTemplate = (args.templateText || '').trim();

  if (safeTemplate) {
    const dedupedTitle = stripEpisodePrefixForTemplate(fallbackTitle, args.episodeNo);
    return safeTemplate
      .replace(/\{episodeNo\}/g, String(args.episodeNo))
      .replace(/\{title\}/g, dedupedTitle);
  }

  return fallbackTitle;
}

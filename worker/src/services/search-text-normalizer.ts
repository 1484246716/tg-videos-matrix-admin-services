/**
 * 搜索文本归一化与构建工具。
 * 为 search-document-builder 提供标题清洗与 search_text 生成能力。
 */

/**
 * 搜索文本归一化工具
 * ─────────────────
 * 将原始文件名/标题清理为可用于全文检索的纯净文本，
 * 并构建合并的 search_text 字段。
 */

// ──────────────────────────── 标题归一化 ────────────────────────────

/**
 * 从原始文件名/标题中提取干净的搜索文本
 * 适配当前项目中常见的文件名格式：
 *   "第1集-《爱美之心》孙涛"
 *   "《策划》赵本山、宋丹丹"
 *   "《演员的烦恼》 表演：赵本山 句号 彤彤"
 */
export function normalizeTitle(raw: string): string {
  return raw
    // 去书名号
    .replace(/[《》]/g, '')
    // 去集数标记（如 "第1集-"、"第12集"、"EP01"）
    .replace(/第\d+集[-\s]*/g, '')
    .replace(/[Ee][Pp]?\d+[-\s]*/g, '')
    // 去分辨率/画质标记
    .replace(/\d{3,4}[pPiI]/g, '')
    .replace(/WEB[-.]?DL|BluRay|HDTV|HDRip|DVDRip|BDRip|x26[45]|HEVC|AAC|FLAC/gi, '')
    // 去文件扩展名
    .replace(/\.(mp4|mkv|avi|rmvb|ts|flv|mov|wmv)$/i, '')
    // 中英文括号统一为空格
    .replace(/[（）()\[\]【】{}]/g, ' ')
    // "表演："、"主演："等前缀去除
    .replace(/(表演|主演|演员|导演)[：:]\s*/g, '')
    // 合并多余空白
    .replace(/\s+/g, ' ')
    .trim();
}

// ──────────────────────────── 搜索文本构建 ────────────────────────────

export interface SearchDocInput {
  title: string;
  originalTitle?: string | null;
  aliases?: string[];
  actors?: string[];
  directors?: string[];
  keywords?: string[];
  description?: string | null;
}

// ?? build Search Text ?????????????????????
export function buildSearchText(doc: SearchDocInput): string {
  const parts: string[] = [];

  // 1. 主标题（最高权重，归一化后放入）
  parts.push(normalizeTitle(doc.title));

  // 2. 原始标题
  if (doc.originalTitle) parts.push(normalizeTitle(doc.originalTitle));

  // 3. 别名
  if (doc.aliases?.length) parts.push(...doc.aliases.map(normalizeTitle));

  // 4. 演员（不做归一化，保留原名）
  if (doc.actors?.length) parts.push(...doc.actors);

  // 5. 导演
  if (doc.directors?.length) parts.push(...doc.directors);

  // 6. 关键词
  if (doc.keywords?.length) parts.push(...doc.keywords);

  // 7. 简介（截取前200字，避免索引膨胀）
  if (doc.description) parts.push(doc.description.slice(0, 200));

  // 全半角统一 + 去重空白
  return parts
    .join(' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// ──────────────────────────── 演员/导演提取 ────────────────────────────

/**
 * 从文件名中提取演员名
 * 当前项目文件名中常见格式如 "《爱美之心》孙涛" 或 "《策划》赵本山、宋丹丹"
 */
export function extractActorsFromTitle(title: string): string[] {
  // 匹配书名号后面的中文名字（用顿号分隔）
  const match = title.match(/》\s*(.+)$/);
  if (!match) return [];

  return match[1]
    .split(/[、,，\s]+/)
    .map(s => s.trim())
    .filter(s => s.length >= 2 && s.length <= 6 && /^[\u4e00-\u9fa5]+$/.test(s));
}

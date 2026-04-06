import type { TagItem } from '../search/search.client';

export function renderTagsPanelMessage(args: {
  page: number;
  pageSize: number;
  total: number;
  tags: TagItem[];
}): string {
  const totalPages = Math.max(1, Math.ceil(args.total / args.pageSize));
  const lines: string[] = [];

  lines.push('<b>电影类型分类频道</b>');
  lines.push(`<b>第 ${args.page}/${totalPages} 页｜共 ${args.total} 个分类</b>`);
  lines.push('点击下方按钮，按分类查看片单。');

  if (args.tags.length === 0) {
    lines.push('当前暂无可用分类。');
  }

  return lines.join('\n');
}

export function renderTagResultMessage(args: {
  tagName: string;
  page: number;
  pageSize: number;
  total: number;
  items: Array<{ title?: string; year?: number | null; actors?: string[]; deepLink?: string }>;
}): string {
  const totalPages = Math.max(1, Math.ceil(args.total / args.pageSize));
  const lines: string[] = [];

  lines.push(`<b>分类：#${escapeHtml(args.tagName)}</b>`);
  lines.push(`<b>第 ${args.page}/${totalPages} 页，共 ${args.total} 条</b>`);
  lines.push('');

  if (args.items.length === 0) {
    lines.push('该分类暂无结果，请切换其他分类。');
    return lines.join('\n');
  }

  args.items.forEach((item, idx) => {
    const no = (args.page - 1) * args.pageSize + idx + 1;
    const title = escapeHtml(item.title || '未命名资源');
    const year = item.year ? ` (${item.year})` : '';
    const actors = item.actors?.length ? `｜主演：${escapeHtml(item.actors.slice(0, 3).join(' / '))}` : '';
    const prefix = `${String(no).padStart(2, '0')}. `;

    if (item.deepLink) {
      lines.push(`${prefix}<a href="${escapeHtml(item.deepLink)}">${title}${year}${actors}</a>`);
    } else {
      lines.push(`${prefix}${title}${year}${actors}`);
    }
  });

  return lines.join('\n');
}

function escapeHtml(input: string): string {
  return input
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

export interface RenderItem {
  title?: string;
  year?: number | null;
  actors?: string[];
  deepLink?: string;
}

export function renderSearchMessage(args: {
  keyword: string;
  page: number;
  pageSize: number;
  total: number;
  items: RenderItem[];
}): string {
  const totalPages = Math.max(1, Math.ceil(args.total / args.pageSize));
  const lines: string[] = [];

  lines.push(`<b>搜索关键词：</b>${escapeHtml(args.keyword)}`);
  lines.push(`<b>第 ${args.page}/${totalPages} 页，共 ${args.total} 条</b>`);
  lines.push('');

  if (args.items.length === 0) {
    lines.push('未找到结果，请尝试更换关键词。');
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

export function renderHotMessage(args: {
  page: number;
  pageSize: number;
  total: number;
  period: '3d' | '7d' | '30d';
  items: Array<{ title?: string; year?: number | null; actors?: string[]; deepLink?: string }>;
}): string {
  const totalPages = Math.max(1, Math.ceil(args.total / args.pageSize));
  const lines: string[] = [];

  lines.push('<b>近期热门影片点播排行榜</b>');
  lines.push(`<b>时间窗：${args.period}｜第 ${args.page}/${totalPages} 页｜共 ${args.total} 条</b>`);
  lines.push('');

  if (args.items.length === 0) {
    lines.push('暂无热门内容，稍后再试。');
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

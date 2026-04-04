export interface RenderItem {
  title?: string;
  year?: number | null;
  actors?: string[];
  telegram_message_link?: string | null;
  telegramMessageLink?: string | null;
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

  lines.push(`搜索关键词：${args.keyword}`);
  lines.push(`第 ${args.page}/${totalPages} 页，共 ${args.total} 条`);
  lines.push('');

  if (args.items.length === 0) {
    lines.push('未找到结果，请尝试更换关键词。');
    return lines.join('\n');
  }

  args.items.forEach((item, idx) => {
    const no = (args.page - 1) * args.pageSize + idx + 1;
    const title = item.title || '未命名资源';
    const year = item.year ? ` (${item.year})` : '';
    const actors = item.actors?.length ? `｜主演：${item.actors.slice(0, 3).join(' / ')}` : '';
    const link = item.telegramMessageLink || item.telegram_message_link;

    lines.push(`${no}. ${title}${year}${actors}`);
    if (link) lines.push(`   链接：${link}`);
  });

  return lines.join('\n');
}

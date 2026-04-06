import type { TagItem } from '../search/search.client';

interface Btn {
  text: string;
  callback_data: string;
}

export function renderTagsKeyboard(args: {
  tags: TagItem[];
  selectTokenByTagId: Map<string, string>;
  selectPrefix?: 'tg:s:' | 'tg:l1:';
  pagerPrefix?: 'tg:m:' | 'tg:l2m:';
  prevToken?: string | null;
  nextToken?: string | null;
}) {
  const rows: Btn[][] = [];
  let pendingRow: Btn[] = [];

  for (const tag of args.tags) {
    const token = args.selectTokenByTagId.get(tag.id);
    if (!token) continue;

    pendingRow.push({
      text: tag.name,
      callback_data: `${args.selectPrefix || 'tg:s:'}${token}`,
    });

    if (pendingRow.length === 2) {
      rows.push(pendingRow);
      pendingRow = [];
    }
  }

  if (pendingRow.length) {
    rows.push(pendingRow);
  }

  const pager: Btn[] = [];
  const pagerPrefix = args.pagerPrefix || 'tg:m:';
  if (args.prevToken) pager.push({ text: '上一页', callback_data: `${pagerPrefix}${args.prevToken}` });
  if (args.nextToken) pager.push({ text: '下一页', callback_data: `${pagerPrefix}${args.nextToken}` });
  if (pager.length) rows.push(pager);

  return rows.length ? { inline_keyboard: rows } : undefined;
}

export function renderTagResultKeyboard(args: {
  menuToken?: string | null;
  prevToken?: string | null;
  nextToken?: string | null;
}) {
  const rows: Btn[][] = [];
  const pager: Btn[] = [];
  if (args.prevToken) pager.push({ text: '上一页', callback_data: `tg:r:${args.prevToken}` });
  if (args.nextToken) pager.push({ text: '下一页', callback_data: `tg:r:${args.nextToken}` });
  if (pager.length) rows.push(pager);

  if (args.menuToken) {
    rows.push([{ text: '返回分类', callback_data: `tg:m:${args.menuToken}` }]);
  }

  return rows.length ? { inline_keyboard: rows } : undefined;
}

export function renderHotKeyboard(args: { prevToken?: string | null; nextToken?: string | null }) {
  const pager: Btn[] = [];
  if (args.prevToken) pager.push({ text: '上一页', callback_data: `rm:p:${args.prevToken}` });
  if (args.nextToken) pager.push({ text: '下一页', callback_data: `rm:p:${args.nextToken}` });
  if (!pager.length) return undefined;
  return { inline_keyboard: [pager] };
}

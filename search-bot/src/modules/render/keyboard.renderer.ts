export interface TelegramInlineKeyboardButton {
  text: string;
  callback_data: string;
}

export interface TelegramInlineKeyboardMarkup {
  inline_keyboard: TelegramInlineKeyboardButton[][];
}

export interface CopyActionButton {
  text: string;
  token: string;
}

export function renderResultKeyboard(args: {
  copyButtons?: CopyActionButton[];
  prevToken?: string | null;
  nextToken?: string | null;
}): TelegramInlineKeyboardMarkup | undefined {
  const rows: TelegramInlineKeyboardButton[][] = [];

  const copyButtons = args.copyButtons || [];

  for (const btn of copyButtons) {
    if (!btn.token) continue;
    rows.push([{ text: btn.text, callback_data: `sc:${btn.token}` }]);
  }

  const pager: TelegramInlineKeyboardButton[] = [];
  if (args.prevToken) pager.push({ text: '上一页', callback_data: `sp:${args.prevToken}` });
  if (args.nextToken) pager.push({ text: '下一页', callback_data: `sp:${args.nextToken}` });
  if (pager.length > 0) rows.push(pager);

  if (rows.length === 0) return undefined;
  return { inline_keyboard: rows };
}

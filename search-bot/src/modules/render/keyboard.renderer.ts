export interface InlineKeyboardButton {
  text: string;
  callback_data: string;
}

export interface InlineKeyboardMarkup {
  inline_keyboard: InlineKeyboardButton[][];
}

export function renderPagerKeyboard(args: {
  prevToken?: string | null;
  nextToken?: string | null;
}): InlineKeyboardMarkup {
  const row: InlineKeyboardButton[] = [];

  if (args.prevToken) {
    row.push({ text: '上一页', callback_data: `sp:${args.prevToken}` });
  }

  if (args.nextToken) {
    row.push({ text: '下一页', callback_data: `sp:${args.nextToken}` });
  }

  return {
    inline_keyboard: row.length > 0 ? [row] : [],
  };
}

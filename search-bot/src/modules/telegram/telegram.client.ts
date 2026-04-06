import axios from 'axios';
import { env } from '../../config/env';

const tg = axios.create({
  baseURL: `https://api.telegram.org/bot${env.BOT_TOKEN}`,
  timeout: 5000,
  headers: {
    'Content-Type': 'application/json',
  },
});

export async function sendMessage(args: {
  chatId: number;
  text: string;
  parseMode?: 'HTML' | 'MarkdownV2';
  replyMarkup?: Record<string, unknown>;
}) {
  return tg.post('/sendMessage', {
    chat_id: args.chatId,
    text: args.text,
    parse_mode: args.parseMode,
    reply_markup: args.replyMarkup,
    disable_web_page_preview: true,
  });
}

export async function editMessage(args: {
  chatId?: number;
  messageId?: number;
  text: string;
  parseMode?: 'HTML' | 'MarkdownV2';
  replyMarkup?: Record<string, unknown>;
}) {
  if (typeof args.chatId !== 'number' || typeof args.messageId !== 'number') {
    return;
  }

  return tg.post('/editMessageText', {
    chat_id: args.chatId,
    message_id: args.messageId,
    text: args.text,
    parse_mode: args.parseMode,
    reply_markup: args.replyMarkup,
    disable_web_page_preview: true,
  });
}

export async function answerCallbackQuery(args: {
  callbackQueryId?: string;
  text?: string;
}) {
  if (!args.callbackQueryId) return;
  return tg.post('/answerCallbackQuery', {
    callback_query_id: args.callbackQueryId,
    text: args.text,
    show_alert: false,
  });
}

export async function copyMessage(args: {
  chatId: string | number;
  fromChatId: string | number;
  messageId: number;
}): Promise<{ message_id: number } | null> {
  const response = await tg.post('/copyMessage', {
    chat_id: args.chatId,
    from_chat_id: args.fromChatId,
    message_id: args.messageId,
  });
  return response.data?.result ?? null;
}

export async function forwardMessage(args: {
  chatId: string | number;
  fromChatId: string | number;
  messageId: number;
}): Promise<{ message_id: number } | null> {
  const response = await tg.post('/forwardMessage', {
    chat_id: args.chatId,
    from_chat_id: args.fromChatId,
    message_id: args.messageId,
  });
  return response.data?.result ?? null;
}

export async function getChatMember(args: {
  chatId: string | number;
  userId: string | number;
}): Promise<{ status?: string; can_post_messages?: boolean } | null> {
  const response = await tg.get('/getChatMember', {
    params: {
      chat_id: args.chatId,
      user_id: args.userId,
    },
  });

  return response.data?.result ?? null;
}

export async function getMe(): Promise<{ id: number } | null> {
  const response = await tg.get('/getMe');
  return response.data?.result ?? null;
}

export async function deleteMessage(args: {
  chatId: number | string;
  messageId: number;
}): Promise<boolean> {
  const response = await tg.post('/deleteMessage', {
    chat_id: args.chatId,
    message_id: args.messageId,
  });

  return Boolean(response.data?.result);
}

export async function setMyCommands(args: {
  commands: Array<{ command: string; description: string }>;
}): Promise<boolean> {
  const response = await tg.post('/setMyCommands', {
    commands: args.commands,
  });

  return Boolean(response.data?.ok);
}

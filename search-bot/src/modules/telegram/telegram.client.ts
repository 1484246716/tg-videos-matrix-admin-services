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
  replyMarkup?: Record<string, unknown>;
}) {
  return tg.post('/sendMessage', {
    chat_id: args.chatId,
    text: args.text,
    reply_markup: args.replyMarkup,
    disable_web_page_preview: true,
  });
}

export async function editMessage(args: {
  chatId?: number;
  messageId?: number;
  text: string;
  replyMarkup?: Record<string, unknown>;
}) {
  if (typeof args.chatId !== 'number' || typeof args.messageId !== 'number') {
    return;
  }

  return tg.post('/editMessageText', {
    chat_id: args.chatId,
    message_id: args.messageId,
    text: args.text,
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

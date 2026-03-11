import { telegramApiBase } from '../config/env';

export type TelegramSendResult = {
  messageId: number;
  messageLink: string | null;
};

export type TelegramError = {
  code: string;
  message: string;
  retryAfterSec?: number;
};

function normalizeTelegramApiBase(raw: string): string {
  return raw.replace(/\/+$/, '');
}

function toTelegramMessageLink(chatIdRaw: string, messageId: number): string | null {
  if (chatIdRaw.startsWith('-100')) {
    const internalId = chatIdRaw.slice(4);
    return `https://t.me/c/${internalId}/${messageId}`;
  }

  if (chatIdRaw.startsWith('@')) {
    return `https://t.me/${chatIdRaw.slice(1)}/${messageId}`;
  }

  return null;
}

export async function sendTelegramRequest(args: {
  botToken: string;
  method: 'sendVideo' | 'sendDocument' | 'sendMessage' | 'pinChatMessage' | 'editMessageText';
  payload: Record<string, unknown> | FormData;
}): Promise<{ messageId?: number; videoFileId?: string; videoFileUniqueId?: string }> {
  const endpoint = `${normalizeTelegramApiBase(telegramApiBase)}/bot${args.botToken}/${args.method}`;

  const isFormData = args.payload instanceof FormData;
  const response = await fetch(endpoint, {
    method: 'POST',
    headers: isFormData
      ? undefined
      : {
        'content-type': 'application/json',
      },
    body: (isFormData ? args.payload : JSON.stringify(args.payload)) as any,
  });

  const json = (await response.json()) as {
    ok: boolean;
    result?: {
      message_id?: number;
      video?: { file_id?: string; file_unique_id?: string };
      document?: { file_id?: string; file_unique_id?: string };
    } | true;
    error_code?: number;
    description?: string;
    parameters?: { retry_after?: number };
  };

  if (!response.ok || !json.ok) {
    const errorCode = json.error_code ?? response.status;
    const description = json.description || `Telegram API HTTP ${response.status}`;

    const err: TelegramError = {
      code: `TG_${errorCode}`,
      message: description,
      retryAfterSec: json.parameters?.retry_after,
    };

    throw err;
  }

  return {
    messageId:
      json.result && typeof json.result === 'object'
        ? json.result.message_id
        : undefined,
    videoFileId:
      json.result && typeof json.result === 'object'
        ? (json.result.video?.file_id ?? json.result.document?.file_id)
        : undefined,
    videoFileUniqueId:
      json.result && typeof json.result === 'object'
        ? (json.result.video?.file_unique_id ?? json.result.document?.file_unique_id)
        : undefined,
  };
}

export async function sendVideoByTelegram(args: {
  botToken: string;
  chatId: string;
  fileId: string;
  caption?: string | null;
  parseMode?: string | null;
  replyMarkup?: unknown;
}): Promise<TelegramSendResult> {
  const payload: Record<string, unknown> = {
    chat_id: args.chatId,
    video: args.fileId,
  };

  if (args.caption) payload.caption = args.caption;
  if (args.parseMode) payload.parse_mode = args.parseMode;
  if (args.replyMarkup) payload.reply_markup = args.replyMarkup;

  const result = await sendTelegramRequest({
    botToken: args.botToken,
    method: 'sendVideo',
    payload,
  });

  return {
    messageId: result.messageId!,
    messageLink: result.messageId ? toTelegramMessageLink(args.chatId, result.messageId) : null,
  };
}

export async function sendTextByTelegram(args: {
  botToken: string;
  chatId: string;
  text: string;
  parseMode?: string;
}): Promise<TelegramSendResult> {
  const result = await sendTelegramRequest({
    botToken: args.botToken,
    method: 'sendMessage',
    payload: {
      chat_id: args.chatId,
      text: args.text,
      parse_mode: args.parseMode ?? 'HTML',
      disable_web_page_preview: true,
    },
  });

  if (!result.messageId) {
    throw new Error('sendMessage response missing message_id');
  }

  return {
    messageId: result.messageId,
    messageLink: toTelegramMessageLink(args.chatId, result.messageId),
  };
}

export async function editMessageTextByTelegram(args: {
  botToken: string;
  chatId: string;
  messageId: number;
  text: string;
  parseMode?: string;
}) {
  const result = await sendTelegramRequest({
    botToken: args.botToken,
    method: 'editMessageText',
    payload: {
      chat_id: args.chatId,
      message_id: args.messageId,
      text: args.text,
      parse_mode: args.parseMode ?? 'HTML',
      disable_web_page_preview: true,
    },
  });

  if (!result.messageId) {
    throw new Error('editMessageText response missing message_id');
  }
}

export async function pinMessageByTelegram(args: {
  botToken: string;
  chatId: string;
  messageId: number;
}) {
  await sendTelegramRequest({
    botToken: args.botToken,
    method: 'pinChatMessage',
    payload: {
      chat_id: args.chatId,
      message_id: args.messageId,
      disable_notification: true,
    },
  });
}

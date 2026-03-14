import axios from 'axios';
import http from 'node:http';
import https from 'node:https';
import { telegramApiBase } from '../config/env';
import { logger, logError } from '../logger';

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

const keepAliveHttpAgent = new http.Agent({
  keepAlive: true,
  keepAliveMsecs: 30000,
  maxSockets: 10,
});

const keepAliveHttpsAgent = new https.Agent({
  keepAlive: true,
  keepAliveMsecs: 30000,
  maxSockets: 10,
});

export type TelegramUpdate = {
  update_id: number;
  message?: {
    message_id: number;
    caption?: string;
    chat?: { id: number | string };
    document?: { file_name?: string };
    video?: { file_name?: string };
  };
  channel_post?: {
    message_id: number;
    caption?: string;
    chat?: { id: number | string };
    document?: { file_name?: string };
    video?: { file_name?: string };
  };
};

export type TelegramRequestMethod =
    | 'sendVideo'
    | 'sendDocument'
    | 'sendMessage'
    | 'pinChatMessage'
    | 'unpinChatMessage'
  | 'editMessageText'
  | 'forwardMessage'
  | 'deleteMessage'
  | 'getUpdates';

export type TelegramRequestArgs = {
  botToken: string;
  method: TelegramRequestMethod;
  payload: Record<string, unknown> | FormData;
};

export type TelegramResponse = {
  messageId?: number;
  videoFileId?: string;
  videoFileUniqueId?: string;
  documentFileId?: string;
  documentFileUniqueId?: string;
  animationFileId?: string;
  animationFileUniqueId?: string;
  updates?: TelegramUpdate[];
};

export async function sendTelegramRequest(args: TelegramRequestArgs): Promise<TelegramResponse> {
  const endpoint = `${normalizeTelegramApiBase(telegramApiBase)}/bot${args.botToken}/${args.method}`;

  const isFormData = args.payload instanceof FormData;
  const timeoutMs = 45 * 60 * 1000;

  let responseStatus = 0;
  let json: {
    ok: boolean;
    result?: {
      message_id?: number;
      video?: { file_id?: string; file_unique_id?: string };
      document?: { file_id?: string; file_unique_id?: string };
      animation?: { file_id?: string; file_unique_id?: string };
    } | true;
    error_code?: number;
    description?: string;
    parameters?: { retry_after?: number };
  };

  try {
    const response = await axios.post(endpoint, args.payload, {
      headers: isFormData ? undefined : { 'content-type': 'application/json' },
      timeout: timeoutMs,
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
      httpAgent: keepAliveHttpAgent,
      httpsAgent: keepAliveHttpsAgent,
      validateStatus: () => true,
    });

    responseStatus = response.status;
    json = response.data as typeof json;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      if (error.code === 'ECONNABORTED') {
      logError('[telegram] 请求超时', {
        method: args.method,
        timeoutMs,
        payloadType: isFormData ? 'form-data' : 'json',
      });
      const err: TelegramError = {
        code: 'TG_TIMEOUT',
        message: `Telegram 请求超时（${timeoutMs}ms）`,
      };
      throw err;
      }

      const isSocketHangUp =
        error.code === 'ECONNRESET' ||
        error.code === 'EPIPE' ||
        (typeof error.message === 'string' && error.message.toLowerCase().includes('socket hang up'));

      if (isSocketHangUp) {
        logError('[telegram] 连接被中断', {
          method: args.method,
          timeoutMs,
          payloadType: isFormData ? 'form-data' : 'json',
          code: error.code ?? null,
          message: error.message,
        });

        const err: TelegramError = {
          code: 'TG_SOCKET_HANG_UP',
          message: 'Telegram 连接中断（socket hang up）',
        };

        throw err;
      }
    }
    throw error;
  }

  if (!json || !json.ok || responseStatus < 200 || responseStatus >= 300) {
    const errorCode = json?.error_code ?? responseStatus;
    const description = json?.description || `Telegram API 请求失败 (${responseStatus})`;

    const err: TelegramError = {
      code: `TG_${errorCode}`,
      message: `Telegram 请求失败：${description}`,
      retryAfterSec: json.parameters?.retry_after,
    };

    logError('[telegram] 请求失败', {
      method: args.method,
      status: responseStatus,
      payloadType: isFormData ? 'form-data' : 'json',
      errorCode,
      description,
      parameters: json?.parameters ?? null,
      response: json,
    });

    throw err;
  }

  logger.info('[telegram] 请求成功', {
    method: args.method,
    status: responseStatus,
    payloadType: isFormData ? 'form-data' : 'json',
    response: json,
  });

  const resultObject = json.result && typeof json.result === 'object' ? json.result : undefined;

  return {
    messageId: resultObject ? resultObject.message_id : undefined,
    videoFileId: resultObject ? resultObject.video?.file_id : undefined,
    videoFileUniqueId: resultObject ? resultObject.video?.file_unique_id : undefined,
    documentFileId: resultObject ? resultObject.document?.file_id : undefined,
    documentFileUniqueId: resultObject ? resultObject.document?.file_unique_id : undefined,
    animationFileId: resultObject ? (resultObject as { animation?: { file_id?: string } }).animation?.file_id : undefined,
    animationFileUniqueId: resultObject ? (resultObject as { animation?: { file_unique_id?: string } }).animation?.file_unique_id : undefined,
    updates: Array.isArray(resultObject) ? (resultObject as TelegramUpdate[]) : undefined,
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
  replyMarkup?: unknown;
}): Promise<TelegramSendResult> {
  const result = await sendTelegramRequest({
    botToken: args.botToken,
    method: 'sendMessage',
    payload: {
      chat_id: args.chatId,
      text: args.text,
      parse_mode: args.parseMode ?? 'HTML',
      disable_web_page_preview: true,
      ...(args.replyMarkup ? { reply_markup: args.replyMarkup } : {}),
    },
  });

  if (!result.messageId) {
    throw new Error('sendMessage 响应缺少 message_id');
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
    throw new Error('editMessageText 响应缺少 message_id');
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

export async function unpinMessageByTelegram(args: {
  botToken: string;
  chatId: string;
  messageId: number;
}) {
  await sendTelegramRequest({
    botToken: args.botToken,
    method: 'unpinChatMessage',
    payload: {
      chat_id: args.chatId,
      message_id: args.messageId,
    },
  });
}

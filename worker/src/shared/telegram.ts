import axios from 'axios';
import http from 'node:http';
import https from 'node:https';
import FormData from 'form-data';
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

function maskTelegramEndpoint(url: string): string {
  return url.replace(/\/bot[^/]+\//, '/bot***\/');
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
    date?: number;
    caption?: string;
    chat?: { id: number | string };
    document?: { file_name?: string; file_size?: number; file_id?: string; file_unique_id?: string };
    video?: { file_name?: string; file_size?: number; file_id?: string; file_unique_id?: string };
  };
  channel_post?: {
    message_id: number;
    date?: number;
    caption?: string;
    chat?: { id: number | string };
    document?: { file_name?: string; file_size?: number; file_id?: string; file_unique_id?: string };
    video?: { file_name?: string; file_size?: number; file_id?: string; file_unique_id?: string };
  };
};

export type TelegramRequestMethod =
    | 'sendVideo'
    | 'sendPhoto'
    | 'sendDocument'
    | 'sendMessage'
    | 'pinChatMessage'
    | 'unpinChatMessage'
  | 'editMessageText'
  | 'forwardMessage'
  | 'deleteMessage'
  | 'getUpdates'
  | 'getChat';

export type TelegramRequestArgs = {
  botToken: string;
  method: TelegramRequestMethod;
  payload: Record<string, unknown> | FormData;
  timeoutMs?: number;
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
  updateIdMax?: number;
};

export async function sendTelegramRequest(args: TelegramRequestArgs): Promise<TelegramResponse> {
  const endpoint = `${normalizeTelegramApiBase(telegramApiBase)}/bot${args.botToken}/${args.method}`;

  const isFormData = args.payload instanceof FormData;
  const timeoutMs = args.timeoutMs ?? 45 * 60 * 1000;

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
    const formHeaders = isFormData && typeof (args.payload as FormData).getHeaders === 'function'
      ? (args.payload as FormData).getHeaders()
      : undefined;
    const response = await axios.post(endpoint, args.payload, {
      headers: isFormData
        ? { ...formHeaders, 'content-length': (args.payload as FormData).getLengthSync() }
        : { 'content-type': 'application/json' },
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
    const rawCode = typeof error === 'object' && error ? (error as { code?: string }).code : undefined;
    const rawMessage = typeof error === 'object' && error ? (error as { message?: string }).message : undefined;
    const errorObj = error as {
      name?: string;
      message?: string;
      code?: string;
      cause?: { message?: string; code?: string };
      errors?: Array<{ message?: string; code?: string }>;
    };
    const isSocketHangUp =
      rawCode === 'ECONNRESET' ||
      rawCode === 'EPIPE' ||
      rawCode === 'TG_SOCKET_HANG_UP' ||
      (typeof rawMessage === 'string' && rawMessage.toLowerCase().includes('socket hang up'));

    const aggregateErrors = Array.isArray(errorObj.errors)
      ? errorObj.errors.map((item) => ({
          message: item?.message ?? null,
          code: item?.code ?? null,
        }))
      : [];

    const causeMessage =
      errorObj.cause && typeof errorObj.cause === 'object' ? (errorObj.cause.message ?? null) : null;
    const causeCode =
      errorObj.cause && typeof errorObj.cause === 'object' ? (errorObj.cause.code ?? null) : null;

    logError('[telegram] 请求异常(网络/连接层)', {
      method: args.method,
      endpoint: maskTelegramEndpoint(endpoint),
      timeoutMs,
      payloadType: isFormData ? 'form-data' : 'json',
      name: errorObj.name ?? null,
      message: errorObj.message ?? null,
      code: errorObj.code ?? null,
      causeMessage,
      causeCode,
      aggregateErrors,
    });

    if (isSocketHangUp) {
      const err: TelegramError = {
        code: 'TG_SOCKET_HANG_UP',
        message: 'Telegram 连接中断（socket hang up）',
      };

      throw err;
    }

    if (axios.isAxiosError(error)) {
      if (error.code === 'ECONNABORTED') {
        logError('[telegram] 请求超时', {
          method: args.method,
          endpoint: maskTelegramEndpoint(endpoint),
          timeoutMs,
          payloadType: isFormData ? 'form-data' : 'json',
        });
        const err: TelegramError = {
          code: 'TG_TIMEOUT',
          message: `Telegram 请求超时（${timeoutMs}ms）`,
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

    const logPayload = {
      method: args.method,
      status: responseStatus,
      payloadType: isFormData ? 'form-data' : 'json',
      errorCode,
      description,
      parameters: json?.parameters ?? null,
      response: json,
    };

    if (args.method === 'deleteMessage' && /message to delete not found/i.test(description)) {
      logger.info('[telegram] 删除消息目标不存在，按已清理处理', logPayload);
    } else {
      logError('[telegram] 请求失败', logPayload);
    }

    throw err;
  }

  logger.info('[telegram] 请求成功', {
    method: args.method,
    status: responseStatus,
    payloadType: isFormData ? 'form-data' : 'json',
    response: json,
  });

  const resultObject = json.result && typeof json.result === 'object' ? json.result : undefined;

  const updates = Array.isArray(json.result) ? (json.result as TelegramUpdate[]) : undefined;
  const updateIdMax = updates && updates.length > 0
    ? updates.reduce((max, update) => (update.update_id > max ? update.update_id : max), updates[0].update_id)
    : undefined;

  return {
    messageId: resultObject ? resultObject.message_id : undefined,
    videoFileId: resultObject ? resultObject.video?.file_id : undefined,
    videoFileUniqueId: resultObject ? resultObject.video?.file_unique_id : undefined,
    documentFileId: resultObject ? resultObject.document?.file_id : undefined,
    documentFileUniqueId: resultObject ? resultObject.document?.file_unique_id : undefined,
    animationFileId: resultObject ? (resultObject as { animation?: { file_id?: string } }).animation?.file_id : undefined,
    animationFileUniqueId: resultObject ? (resultObject as { animation?: { file_unique_id?: string } }).animation?.file_unique_id : undefined,
    updates,
    updateIdMax,
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

export async function sendPhotoByTelegram(args: {
  botToken: string;
  chatId: string;
  fileId: string;
  caption?: string | null;
  parseMode?: string | null;
  replyMarkup?: unknown;
}): Promise<TelegramSendResult> {
  const payload: Record<string, unknown> = {
    chat_id: args.chatId,
    photo: args.fileId,
  };

  if (args.caption) payload.caption = args.caption;
  if (args.parseMode) payload.parse_mode = args.parseMode;
  if (args.replyMarkup) payload.reply_markup = args.replyMarkup;

  const result = await sendTelegramRequest({
    botToken: args.botToken,
    method: 'sendPhoto',
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
  disableWebPagePreview?: boolean;
}): Promise<TelegramSendResult> {
  const result = await sendTelegramRequest({
    botToken: args.botToken,
    method: 'sendMessage',
    payload: {
      chat_id: args.chatId,
      text: args.text,
      parse_mode: args.parseMode ?? 'HTML',
      disable_web_page_preview: args.disableWebPagePreview ?? true,
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

export async function getTelegramUpdates(args: {
  botToken: string;
  offset?: number;
  limit?: number;
  timeoutSec?: number;
  allowedUpdates?: string[];
}): Promise<{ updates: TelegramUpdate[]; updateIdMax?: number }> {
  const payload: Record<string, unknown> = {
    ...(args.offset ? { offset: args.offset } : {}),
    ...(args.limit ? { limit: args.limit } : {}),
    ...(args.timeoutSec ? { timeout: args.timeoutSec } : {}),
    ...(args.allowedUpdates ? { allowed_updates: args.allowedUpdates } : {}),
  };

  const result = await sendTelegramRequest({
    botToken: args.botToken,
    method: 'getUpdates',
    payload,
    timeoutMs: (args.timeoutSec ?? 5) * 1000,
  });

  return { updates: result.updates ?? [], updateIdMax: result.updateIdMax };
}

export async function editMessageTextByTelegram(args: {
  botToken: string;
  chatId: string;
  messageId: number;
  text: string;
  parseMode?: string;
  replyMarkup?: unknown;
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
      ...(args.replyMarkup ? { reply_markup: args.replyMarkup } : {}),
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

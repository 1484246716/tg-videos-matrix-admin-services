/**
 * Telegram 请求封装：统一处理 Bot API 调用、重试与错误归一化。
 * 为 relay / dispatch / catalog / mass-message 提供稳定的 Telegram 发送能力。
 */

import axios from 'axios';
import http from 'node:http';
import https from 'node:https';
import FormData from 'form-data';
import {
  telegramApiBase,
  TG_RETRY_BACKOFF_MAX_SECONDS,
  TG_RETRY_MAX_ATTEMPTS,
  TG_SEND_MIN_INTERVAL_MS,
} from '../config/env';
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

// 规范化 Telegram API Base（移除末尾斜杠）。
function normalizeTelegramApiBase(raw: string): string {
  return raw.replace(/\/+$/, '');
}

// 脱敏日志中的 Telegram endpoint（隐藏 bot token）。
function maskTelegramEndpoint(url: string): string {
  return url.replace(/\/bot[^/]+\//, '/bot***\/');
}

// 将 chatId/messageId 转换为可访问的消息链接。
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
    | 'sendMediaGroup'
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
  messageIds?: number[];
  mediaGroupId?: string;
  videoFileId?: string;
  videoFileUniqueId?: string;
  photoFileId?: string;
  photoFileUniqueId?: string;
  documentFileId?: string;
  documentFileUniqueId?: string;
  animationFileId?: string;
  animationFileUniqueId?: string;
  updates?: TelegramUpdate[];
  updateIdMax?: number;
};

// 简单异步等待函数。
function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

let lastTelegramRequestAt = 0;
// 控制 Telegram 请求频率，避免突发过载。
async function throttleTelegramRequests() {
  if (TG_SEND_MIN_INTERVAL_MS <= 0) return;
  const now = Date.now();
  const waitMs = Math.max(0, lastTelegramRequestAt + TG_SEND_MIN_INTERVAL_MS - now);
  if (waitMs > 0) {
    await sleep(waitMs);
  }
  lastTelegramRequestAt = Date.now();
}

// 从错误描述中提取 retry_after 秒数。
function parseRetryAfterSeconds(description: string, fallback?: number): number | undefined {
  if (typeof fallback === 'number' && Number.isFinite(fallback) && fallback > 0) {
    return Math.floor(fallback);
  }
  const m = description.match(/retry after\s+(\d+)/i);
  if (!m) return undefined;
  const n = Number(m[1]);
  if (!Number.isFinite(n) || n <= 0) return undefined;
  return Math.floor(n);
}

// 计算重试延迟（优先 retry_after，其次指数退避）。
function computeRetryDelayMs(attempt: number, retryAfterSec?: number) {
  if (retryAfterSec && retryAfterSec > 0) {
    const jitter = Math.floor(Math.random() * 250);
    return retryAfterSec * 1000 + jitter;
  }

  const expSec = Math.min(TG_RETRY_BACKOFF_MAX_SECONDS, Math.pow(2, Math.max(0, attempt - 1)));
  const jitter = Math.floor(Math.random() * 250);
  return expSec * 1000 + jitter;
}

// 将对象安全序列化为日志字符串。
function stringifyForLog(value: unknown): string | null {
  if (value === undefined || value === null) return null;
  if (typeof value === 'string') return value;

  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

// 统一发送 Telegram 请求（含限流、重试、错误归一化）。
export async function sendTelegramRequest(args: TelegramRequestArgs): Promise<TelegramResponse> {
  const endpoint = `${normalizeTelegramApiBase(telegramApiBase)}/bot${args.botToken}/${args.method}`;

  const isFormData = args.payload instanceof FormData;
  const timeoutMs = args.timeoutMs ?? 45 * 60 * 1000;

  let responseStatus = 0;
  let json:
    | {
        ok: boolean;
        result?:
          | {
              message_id?: number;
              video?: { file_id?: string; file_unique_id?: string };
              document?: { file_id?: string; file_unique_id?: string };
              animation?: { file_id?: string; file_unique_id?: string };
            }
          | TelegramUpdate[]
          | true;
        error_code?: number;
        description?: string;
        parameters?: { retry_after?: number };
      }
    | undefined;

  for (let attempt = 1; attempt <= TG_RETRY_MAX_ATTEMPTS; attempt += 1) {
    await throttleTelegramRequests();

    try {
      const formHeaders = isFormData && typeof (args.payload as FormData).getHeaders === 'function'
        ? (args.payload as FormData).getHeaders()
        : undefined;
      const response = await axios.post(endpoint, args.payload, {
        headers: isFormData
          ? { ...formHeaders }
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

      const isTimeout = axios.isAxiosError(error) && error.code === 'ECONNABORTED';
      const isRetryableNetworkError = isSocketHangUp || isTimeout;

      if (isRetryableNetworkError && attempt < TG_RETRY_MAX_ATTEMPTS) {
        const delayMs = computeRetryDelayMs(attempt);
        logger.warn('[telegram] 网络层异常，准备重试', {
          method: args.method,
          attempt,
          maxAttempts: TG_RETRY_MAX_ATTEMPTS,
          delayMs,
          endpoint: maskTelegramEndpoint(endpoint),
          isSocketHangUp,
          isTimeout,
        });
        await sleep(delayMs);
        continue;
      }

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
        attempt,
      });

      if (isSocketHangUp) {
        const err: TelegramError = {
          code: 'TG_SOCKET_HANG_UP',
          message: 'Telegram 连接中断（socket hang up）',
        };
        throw err;
      }

      if (isTimeout) {
        const err: TelegramError = {
          code: 'TG_TIMEOUT',
          message: `Telegram 请求超时（${timeoutMs}ms）`,
        };
        throw err;
      }

      throw error;
    }

    if (!json || !json.ok || responseStatus < 200 || responseStatus >= 300) {
      const errorCode = json?.error_code ?? responseStatus;
      const description = json?.description || `Telegram API 请求失败 (${responseStatus})`;
      const retryAfterSec = parseRetryAfterSeconds(description, json?.parameters?.retry_after ?? undefined);

      const err: TelegramError = {
        code: `TG_${errorCode}`,
        message: `Telegram 请求失败：${description}`,
        retryAfterSec,
      };

      const responseRaw = stringifyForLog(json);
      const logPayload = {
        method: args.method,
        status: responseStatus,
        payloadType: isFormData ? 'form-data' : 'json',
        errorCode,
        description,
        parameters: json?.parameters ?? null,
        response: json,
        responseRaw,
        responseType: typeof json,
        attempt,
      };

      if (args.method === 'deleteMessage' && /message to delete not found/i.test(description)) {
        logger.info('[telegram] 删除消息目标不存在，按已清理处理', logPayload);
        throw err;
      }

      if (args.method === 'editMessageText' && /message is not modified/i.test(description)) {
        logger.info('[telegram] 编辑消息内容未变化，按幂等成功处理', logPayload);
        throw err;
      }

      const isTooManyRequests = Number(errorCode) === 429 || /too many requests/i.test(description);
      if (isTooManyRequests && attempt < TG_RETRY_MAX_ATTEMPTS) {
        const delayMs = computeRetryDelayMs(attempt, retryAfterSec ?? undefined);
        logger.warn('[telegram] 触发限流，准备重试', {
          ...logPayload,
          retryAfterSec: retryAfterSec ?? null,
          delayMs,
        });
        await sleep(delayMs);
        continue;
      }

      if (Number(errorCode) === 400) {
        logger.error('[telegram] 请求失败(400详细响应)', {
          ...logPayload,
          hint: '请优先查看 responseRaw / description / parameters 定位非法参数',
        });
      }

      logError('[telegram] 请求失败', logPayload);
      throw err;
    }

    break;
  }

  if (!json || !json.ok || responseStatus < 200 || responseStatus >= 300) {
    throw new Error('[telegram] 请求失败：重试后未获得成功响应');
  }

  logger.info('[telegram] 请求成功', {
    method: args.method,
    status: responseStatus,
    payloadType: isFormData ? 'form-data' : 'json',
    response: json,
  });

  const updates = Array.isArray(json.result) ? (json.result as TelegramUpdate[]) : undefined;
  const updateIdMax = updates && updates.length > 0
    ? updates.reduce((max, update) => (update.update_id > max ? update.update_id : max), updates[0].update_id)
    : undefined;

  const resultArray =
    Array.isArray(json.result)
      ? (json.result as Array<{
          message_id?: number;
          media_group_id?: string;
          video?: { file_id?: string; file_unique_id?: string };
          photo?: Array<{ file_id?: string; file_unique_id?: string }>;
          document?: { file_id?: string; file_unique_id?: string };
          animation?: { file_id?: string; file_unique_id?: string };
        }>)
      : undefined;

  const resultObject =
    !Array.isArray(json.result) && json.result && typeof json.result === 'object'
      ? (json.result as {
          message_id?: number;
          media_group_id?: string;
          video?: { file_id?: string; file_unique_id?: string };
          photo?: Array<{ file_id?: string; file_unique_id?: string }>;
          document?: { file_id?: string; file_unique_id?: string };
          animation?: { file_id?: string; file_unique_id?: string };
        })
      : undefined;

  const photoVariants = resultObject?.photo;
  const photoLargest = Array.isArray(photoVariants) && photoVariants.length > 0
    ? photoVariants[photoVariants.length - 1]
    : undefined;

  return {
    messageId: resultObject?.message_id,
    messageIds: resultArray?.map((x) => Number(x.message_id)).filter((n) => Number.isFinite(n)),
    mediaGroupId: resultObject?.media_group_id ?? resultArray?.[0]?.media_group_id,
    videoFileId: resultObject?.video?.file_id,
    videoFileUniqueId: resultObject?.video?.file_unique_id,
    photoFileId: photoLargest?.file_id,
    photoFileUniqueId: photoLargest?.file_unique_id,
    documentFileId: resultObject?.document?.file_id,
    documentFileUniqueId: resultObject?.document?.file_unique_id,
    animationFileId: resultObject?.animation?.file_id,
    animationFileUniqueId: resultObject?.animation?.file_unique_id,
    updates,
    updateIdMax,
  };
}

// 通过 Telegram 发送视频（使用 file_id）。
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

// 通过 Telegram 发送图片（使用 file_id）。
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

// 通过 Telegram 发送文本消息。
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

// 调用 getUpdates 获取 Telegram 更新流。
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

// 通过 Telegram 编辑消息文本。
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

// 通过 Telegram 置顶消息。
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

// 通过 Telegram 取消置顶消息。
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

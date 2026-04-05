import axios from 'axios';
import { verifyDeepLinkToken } from '../deeplink/deeplink.service';
import { copyMessage, forwardMessage, sendMessage, getChatMember, getMe } from '../telegram/telegram.client';
import { env } from '../../config/env';
import { setIfAbsent } from '../../infra/redis';

function isRetryableError(error: unknown): boolean {
  if (!axios.isAxiosError(error)) return false;
  const status = error.response?.status;
  return status === 429 || (typeof status === 'number' && status >= 500);
}

function getRetryAfterMs(error: unknown): number {
  if (!axios.isAxiosError(error)) return env.SEARCH_BOT_COPY_RETRY_BACKOFF_MS;
  const retryAfter = Number(
    (error.response?.data as { parameters?: { retry_after?: number } } | undefined)?.parameters?.retry_after,
  );
  if (Number.isFinite(retryAfter) && retryAfter > 0) return retryAfter * 1000;
  return env.SEARCH_BOT_COPY_RETRY_BACKOFF_MS;
}

async function withRetry<T>(fn: () => Promise<T>): Promise<T> {
  let attempt = 0;
  let lastError: unknown;

  while (attempt <= env.SEARCH_BOT_COPY_RETRY_MAX) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (!isRetryableError(error) || attempt >= env.SEARCH_BOT_COPY_RETRY_MAX) {
        break;
      }
      const backoff = getRetryAfterMs(error) * (attempt + 1);
      await new Promise((resolve) => setTimeout(resolve, backoff));
      attempt += 1;
    }
  }

  throw lastError;
}

async function ensureBotPermission(chatId: string): Promise<boolean> {
  const me = await getMe();
  if (!me?.id) return false;
  const member = await getChatMember({ chatId, userId: me.id });
  if (!member?.status) return false;
  if (member.status === 'administrator' || member.status === 'creator') return true;
  return Boolean(member.can_post_messages);
}

export async function handleStartCommand(args: {
  chatId: number;
  fromId?: number;
  text?: string;
}) {
  const text = args.text || '';
  const match = text.match(/^\/start\s+cp_(\S+)/);
  if (!match) {
    return {
      routed: 'start',
      ok: true,
      action: 'send_message',
      send: {
        chatId: args.chatId,
        text: '欢迎使用搜索机器人。请在频道中使用 /s 关键词 进行搜索。',
      },
    };
  }

  const shortToken = match[1];
  const verify = await verifyDeepLinkToken(shortToken, args.fromId ? String(args.fromId) : undefined);
  if (!verify.ok) {
    return {
      routed: 'start',
      ok: true,
      action: 'send_message',
      send: {
        chatId: args.chatId,
        text:
          verify.reason === 'expired'
            ? '链接已过期，请返回频道重新搜索。'
            : verify.reason === 'forbidden'
              ? '你无权触发该发送链接。'
              : verify.reason === 'replayed'
                ? '该链接已使用，请勿重复点击。'
                : '链接无效，请重新获取。',
      },
    };
  }

  const state = verify.state;

  const idemKey = `sb:copy:idem:${state.fromChatId}:${state.messageId}:${state.targetChatId}`;
  const firstCopy = await setIfAbsent(idemKey, '1', env.SEARCH_BOT_COPY_IDEMPOTENT_TTL_SEC);
  if (!firstCopy) {
    return {
      routed: 'start',
      ok: true,
      action: 'send_message',
      send: {
        chatId: args.chatId,
        text: '该资源近期已发送，无需重复操作。',
      },
    };
  }

  const [sourceAllowed, targetAllowed] = await Promise.all([
    ensureBotPermission(state.fromChatId),
    ensureBotPermission(state.targetChatId),
  ]);

  if (!sourceAllowed || !targetAllowed) {
    return {
      routed: 'start',
      ok: true,
      action: 'send_message',
      send: {
        chatId: args.chatId,
        text: '机器人权限不足，无法发送该资源，请联系管理员。',
      },
    };
  }

  try {
    await withRetry(() => copyMessage({ chatId: state.targetChatId, fromChatId: state.fromChatId, messageId: state.messageId }));
    await sendMessage({
      chatId: Number(state.targetChatId),
      text: `✅ 已由搜索机器人发送：${state.title || '资源消息'}`,
    });

    return {
      routed: 'start',
      ok: true,
      action: 'send_message',
      send: {
        chatId: args.chatId,
        text: '发送成功，已投递到目标频道。',
      },
    };
  } catch {
    try {
      await withRetry(() => forwardMessage({ chatId: state.targetChatId, fromChatId: state.fromChatId, messageId: state.messageId }));
      return {
        routed: 'start',
        ok: true,
        action: 'send_message',
        send: {
          chatId: args.chatId,
          text: 'copy失败，已自动转发到目标频道。',
        },
      };
    } catch {
      return {
        routed: 'start',
        ok: true,
        action: 'send_message',
        send: {
          chatId: args.chatId,
          text: state.telegramMessageLink
            ? `发送失败，可直接查看原消息：${state.telegramMessageLink}`
            : '发送失败，请稍后重试。',
        },
      };
    }
  }
}

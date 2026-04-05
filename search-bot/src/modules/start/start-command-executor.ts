import axios from 'axios';
import { verifyDeepLinkToken } from '../deeplink/deeplink.service';
import { copyMessage, forwardMessage, sendMessage, getChatMember, getMe } from '../telegram/telegram.client';
import { env } from '../../config/env';
import { setIfAbsent } from '../../infra/redis';
import { logger } from '../../infra/logger';

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

export async function executeStartPayload(args: {
  chatId: number;
  fromId?: number;
  text: string;
  consumeNonce: boolean;
  withPrivateAck?: boolean;
}) {
  const match = args.text.match(/^\/start\s+cp_(\S+)/);
  if (!match) {
    return {
      ok: false,
      message: 'invalid_start_payload',
    } as const;
  }

  const shortToken = match[1];

  logger.info('开始执行频道发送', {
    shortToken,
    chatId: args.chatId,
    fromId: args.fromId,
    consumeNonce: args.consumeNonce,
  });

  const verify = await verifyDeepLinkToken(shortToken, args.fromId ? String(args.fromId) : undefined, {
    consumeNonce: args.consumeNonce,
  });

  if (!verify.ok) {
    logger.warn('频道发送校验失败', {
      shortToken,
      reason: verify.reason,
    });

    const message =
      verify.reason === 'expired'
        ? '链接已过期，请返回频道重新搜索。'
        : verify.reason === 'forbidden'
          ? '你无权触发该发送链接。'
          : verify.reason === 'replayed'
            ? '该链接已使用，请勿重复点击。'
            : '链接无效，请重新获取。';

    if (args.withPrivateAck) {
      await sendMessage({
        chatId: args.chatId,
        text: message,
      });
    }

    return {
      ok: false,
      message,
      reason: verify.reason,
    } as const;
  }

  const state = verify.state;

  logger.info('频道发送校验通过', {
    shortToken,
    fromChatId: state.fromChatId,
    targetChatId: state.targetChatId,
    messageId: state.messageId,
  });

  const idemKey = `sb:copy:idem:${state.fromChatId}:${state.messageId}:${state.targetChatId}`;
  const firstCopy = await setIfAbsent(idemKey, '1', env.SEARCH_BOT_COPY_IDEMPOTENT_TTL_SEC);
  if (!firstCopy) {
    logger.warn('频道发送命中幂等去重', {
      shortToken,
      idemKey,
    });

    const message = '该资源近期已发送，无需重复操作。';

    if (args.withPrivateAck) {
      await sendMessage({ chatId: args.chatId, text: message });
    }

    return { ok: false, message, reason: 'duplicated' as const };
  }

  const [sourceAllowed, targetAllowed] = await Promise.all([
    ensureBotPermission(state.fromChatId),
    ensureBotPermission(state.targetChatId),
  ]);

  if (!sourceAllowed || !targetAllowed) {
    logger.warn('频道发送权限不足', {
      shortToken,
      sourceAllowed,
      targetAllowed,
      fromChatId: state.fromChatId,
      targetChatId: state.targetChatId,
    });

    const message = '机器人权限不足，无法发送该资源，请联系管理员。';

    if (args.withPrivateAck) {
      await sendMessage({ chatId: args.chatId, text: message });
    }

    return { ok: false, message, reason: 'permission_denied' as const };
  }

  try {
    logger.info('频道发送copy开始', {
      shortToken,
      fromChatId: state.fromChatId,
      targetChatId: state.targetChatId,
      messageId: state.messageId,
    });

    await withRetry(() =>
      copyMessage({
        chatId: state.targetChatId,
        fromChatId: state.fromChatId,
        messageId: state.messageId,
      }),
    );

    await sendMessage({
      chatId: Number(state.targetChatId),
      text: `✅ 已由搜索机器人发送：${state.title || '资源消息'}`,
    });

    logger.info('start.execute_copy_done', {
      shortToken,
      targetChatId: state.targetChatId,
    });

    if (args.withPrivateAck) {
      await sendMessage({
        chatId: args.chatId,
        text: '发送成功，已投递到目标频道。',
      });
    }

    return { ok: true, message: 'sent' as const };
  } catch (copyError) {
    logger.warn('start.execute_copy_failed', {
      shortToken,
      error: copyError instanceof Error ? copyError.message : String(copyError),
    });

    try {
      await withRetry(() =>
        forwardMessage({
          chatId: state.targetChatId,
          fromChatId: state.fromChatId,
          messageId: state.messageId,
        }),
      );

      if (args.withPrivateAck) {
        await sendMessage({
          chatId: args.chatId,
          text: 'copy失败，已自动转发到目标频道。',
        });
      }

      logger.info('start.execute_forward_done', {
        shortToken,
        targetChatId: state.targetChatId,
      });

      return { ok: true, message: 'forwarded' as const };
    } catch (forwardError) {
      logger.error('start.execute_forward_failed', {
        shortToken,
        error: forwardError instanceof Error ? forwardError.message : String(forwardError),
        fallbackLink: state.telegramMessageLink || null,
      });

      const message = state.telegramMessageLink
        ? `发送失败，可直接查看原消息：${state.telegramMessageLink}`
        : '发送失败，请稍后重试。';

      if (args.withPrivateAck) {
        await sendMessage({ chatId: args.chatId, text: message });
      }

      return { ok: false, message, reason: 'failed' as const };
    }
  }
}

import axios from 'axios';
import { verifyDeepLinkToken } from '../deeplink/deeplink.service';
import { copyMessage, forwardMessage, sendMessage, getChatMember, getMe, deleteMessage } from '../telegram/telegram.client';
import { env } from '../../config/env';
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

async function ensureBotPermission(chatId: string, mode: 'source' | 'target'): Promise<boolean> {
  const chatIdNumber = Number(chatId);

  // 私聊目标：不需要频道发帖权限，直接视为可发送
  if (mode === 'target' && Number.isFinite(chatIdNumber) && chatIdNumber > 0) {
    return true;
  }

  const me = await getMe();
  if (!me?.id) return false;

  const member = await getChatMember({ chatId, userId: me.id });
  if (!member?.status) return false;

  if (member.status === 'administrator' || member.status === 'creator' || member.status === 'member') {
    return true;
  }

  if (typeof member.can_post_messages === 'boolean') {
    return member.can_post_messages;
  }

  return false;
}

export async function executeStartPayload(args: {
  chatId: number;
  fromId?: number;
  text: string;
  consumeNonce: boolean;
  withPrivateAck?: boolean;
  triggerMessageId?: number;
  deleteTriggerMessage?: boolean;
}) {
  const match = args.text.match(/^\/start\s+cp_(\S+)/);
  if (!match) {
    return {
      ok: false,
      message: 'invalid_start_payload',
    } as const;
  }

  const shortToken = match[1];

  logger.info('开始执行私聊发送', {
    shortToken,
    chatId: args.chatId,
    fromId: args.fromId,
    consumeNonce: args.consumeNonce,
  });

  if (args.deleteTriggerMessage && env.SEARCH_BOT_START_DELETE_TRIGGER_MESSAGE && args.triggerMessageId) {
    try {
      await deleteMessage({ chatId: args.chatId, messageId: args.triggerMessageId });
      logger.info('start触发消息删除成功', {
        chatId: args.chatId,
        messageId: args.triggerMessageId,
      });
    } catch (error) {
      logger.warn('start触发消息删除失败', {
        chatId: args.chatId,
        messageId: args.triggerMessageId,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  const verify = await verifyDeepLinkToken(shortToken, args.fromId ? String(args.fromId) : undefined, {
    consumeNonce: args.consumeNonce,
  });

  if (!verify.ok) {
    logger.warn('私聊发送校验失败', {
      shortToken,
      reason: verify.reason,
    });

    const message =
      verify.reason === 'expired'
        ? '链接已过期，请重新搜索。'
        : verify.reason === 'forbidden'
          ? '你无权触发该发送链接。'
          : verify.reason === 'replayed'
            ? '该链接已使用，请勿重复点击。'
            : '链接无效，请重新获取。';

    if (args.withPrivateAck !== false) {
      await sendMessage({ chatId: args.chatId, text: message });
    }

    return { ok: false, message, reason: verify.reason } as const;
  }

  const state = verify.state;

  logger.info('私聊发送校验通过', {
    shortToken,
    fromChatId: state.fromChatId,
    targetChatId: state.targetChatId,
    messageId: state.messageId,
  });


  const [sourceAllowed, targetAllowed] = await Promise.all([
    ensureBotPermission(state.fromChatId, 'source'),
    ensureBotPermission(state.targetChatId, 'target'),
  ]);

  if (!sourceAllowed || !targetAllowed) {
    logger.warn('私聊发送权限不足', {
      shortToken,
      sourceAllowed,
      targetAllowed,
      fromChatId: state.fromChatId,
      targetChatId: state.targetChatId,
    });

    const message = '机器人权限不足，无法发送该资源，请联系管理员。';

    if (args.withPrivateAck !== false) {
      await sendMessage({ chatId: args.chatId, text: message });
    }

    return { ok: false, message, reason: 'permission_denied' as const };
  }

  try {
    logger.info('私聊发送copy开始', {
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

    logger.info('私聊发送copy完成', {
      shortToken,
      targetChatId: state.targetChatId,
    });

    return { ok: true, message: 'sent' as const };
  } catch (copyError) {
    logger.warn('私聊发送copy失败', {
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

      logger.info('私聊发送forward完成', {
        shortToken,
        targetChatId: state.targetChatId,
      });

      return { ok: true, message: 'forwarded' as const };
    } catch (forwardError) {
      logger.error('私聊发送forward失败', {
        shortToken,
        error: forwardError instanceof Error ? forwardError.message : String(forwardError),
        fallbackLink: state.telegramMessageLink || null,
      });

      const message = state.telegramMessageLink
        ? `发送失败，可直接查看原消息：${state.telegramMessageLink}`
        : '发送失败，请稍后重试。';

      if (args.withPrivateAck !== false) {
        await sendMessage({ chatId: args.chatId, text: message });
      }

      return { ok: false, message, reason: 'failed' as const };
    }
  }
}

/**
 * Clone Channels 会话服务：统一获取 Telegram 客户端并封装超时与错误归一化。
 * 用于在 clone 调度/执行链路中提供稳定的 user/bot 会话访问入口。
 */

import { TelegramClient } from 'telegram';
import { logger } from '../../logger';
import { getGramjsBotClient, getGramjsUserClient } from '../../shared/gramjs/client';

export type CloneAccountType = 'user' | 'bot';

type GetClientParams = {
  accountId?: string;
  forceReconnect?: boolean;
  accountType?: CloneAccountType;
};

type WithClientParams = {
  accountId?: string;
  timeoutMs?: number;
  accountType?: CloneAccountType;
};

// 为异步调用附加超时控制。
async function runWithTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return promise;

  return await Promise.race<T>([
    promise,
    new Promise<T>((_resolve, reject) => {
      const timer = setTimeout(() => {
        clearTimeout(timer);
        reject(new Error(`clone session timeout after ${timeoutMs}ms`));
      }, timeoutMs);
    }),
  ]);
}

// 统一归一化 clone 相关错误，便于上层重试策略判断。
function normalizeCloneError(err: unknown): Error {
  const message = err instanceof Error ? err.message : String(err);
  const lowered = message.toLowerCase();

  if (lowered.includes('auth') || lowered.includes('session')) {
    return new Error(`auth_invalid: ${message}`);
  }

  if (lowered.includes('floodwait') || lowered.includes('flood_wait')) {
    return new Error(`flood_wait: ${message}`);
  }

  if (lowered.includes('timeout') || lowered.includes('network') || lowered.includes('socket')) {
    return new Error(`network_timeout: ${message}`);
  }

  return err instanceof Error ? err : new Error(message);
}

// 获取可用 Telegram 客户端（支持 user/bot 类型）。
export async function getClient(params?: GetClientParams): Promise<TelegramClient> {
  if (params?.forceReconnect) {
    await disconnectClient(params.accountId);
  }

  const accountType = params?.accountType ?? 'user';

  try {
    const client = accountType === 'bot'
      ? await getGramjsBotClient()
      : await getGramjsUserClient();
    return client;
  } catch (err) {
    throw normalizeCloneError(err);
  }
}

// 以统一日志/超时/错误处理包装客户端调用。
export async function withClient<T>(
  params: WithClientParams,
  fn: (client: TelegramClient) => Promise<T>,
): Promise<T> {
  const startedAt = Date.now();

  try {
    const accountType = params.accountType ?? 'user';
    const client = await getClient({ accountId: params.accountId, accountType });
    const timeoutMs = params.timeoutMs ?? 120_000;
    const result = await runWithTimeout(fn(client), timeoutMs);

    logger.info('[clone][session] withClient success', {
      accountType,
      accountId: params.accountId ?? null,
      elapsedMs: Date.now() - startedAt,
    });

    return result;
  } catch (err) {
    const normalized = normalizeCloneError(err);
    logger.warn('[clone][session] withClient failed', {
      accountType: params.accountType ?? 'user',
      accountId: params.accountId ?? null,
      elapsedMs: Date.now() - startedAt,
      error: normalized.message,
    });
    throw normalized;
  }
}

// 兼容断连入口：当前共享客户端实现下为 no-op。
export async function disconnectClient(_accountId?: string): Promise<void> {
  // 目前 shared/gramjs/client.ts 维护了单例的 Bot/User 客户端，但未暴露安全的细粒度断开接口。
  // 这里先保留兼容签名，后续若 client 层支持按 accountId disconnect，再接入真实释放逻辑。
  logger.info('[clone][session] disconnect requested (no-op with current shared client)');
}

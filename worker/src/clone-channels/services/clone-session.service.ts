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

export async function disconnectClient(_accountId?: string): Promise<void> {
  // 目前 shared/gramjs/client.ts 仅暴露单例 getGramjsClient，未暴露安全断开接口。
  // 这里先保留兼容签名，后续若 client 层支持 disconnect，再接入真实释放逻辑。
  logger.info('[clone][session] disconnect requested (no-op with current shared client)');
}

import { CHANNEL_LOCK_ENABLED, CHANNEL_LOCK_TTL_MS } from '../config/env';
import { connection } from '../infra/redis';

function buildLockKey(scope: 'dispatch' | 'catalog', channelId: bigint) {
  return `lock:${scope}:channel:${channelId.toString()}`;
}

export async function tryAcquireChannelLock(params: {
  scope: 'dispatch' | 'catalog';
  channelId: bigint;
}) {
  if (!CHANNEL_LOCK_ENABLED) {
    return {
      enabled: false,
      lockKey: buildLockKey(params.scope, params.channelId),
      lockToken: null as string | null,
      acquired: true,
    };
  }

  const lockKey = buildLockKey(params.scope, params.channelId);
  const lockToken = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const result = await connection.set(lockKey, lockToken, 'PX', CHANNEL_LOCK_TTL_MS, 'NX');

  return {
    enabled: true,
    lockKey,
    lockToken,
    acquired: result === 'OK',
  };
}

export async function releaseChannelLock(params: {
  lockKey: string;
  lockToken: string | null;
}) {
  if (!CHANNEL_LOCK_ENABLED || !params.lockToken) return;

  const lua = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
    end
    return 0
  `;

  await connection.eval(lua, 1, params.lockKey, params.lockToken);
}

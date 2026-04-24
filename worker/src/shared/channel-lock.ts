/**
 * 频道级分布式锁工具：供 scheduler/worker/service 共享使用。
 * 用于限制同一频道并发执行，避免重复调度与竞争写入。
 */

import { CHANNEL_LOCK_ENABLED, CHANNEL_LOCK_TTL_MS } from '../config/env';
import { connection } from '../infra/redis';

// 构造频道锁 key。
function buildLockKey(scope: 'dispatch' | 'catalog', channelId: bigint) {
  return `lock:${scope}:channel:${channelId.toString()}`;
}

// 尝试获取频道锁。
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

// 释放频道锁（仅在 token 匹配时删除）。
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

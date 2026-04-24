/**
 * 本地中转路径锁工具：供 scheduler/worker/service 共享使用。
 * 防止同一路径被并发上传或重复处理造成竞争冲突。
 */

import { createHash } from 'node:crypto';
import { connection } from '../infra/redis';
import { RELAY_LOCAL_PATH_LOCK_TTL_MS } from '../config/env';

// 构建本地路径锁 key（路径哈希）。
function buildRelayPathLockKey(localPath: string) {
  const digest = createHash('sha1').update(localPath.toLowerCase()).digest('hex');
  return `lock:relay:path:${digest}`;
}

// 尝试获取本地路径锁。
export async function tryAcquireRelayPathLock(localPath: string) {
  const lockKey = buildRelayPathLockKey(localPath);

  const lockToken = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const result = await connection.set(lockKey, lockToken, 'PX', RELAY_LOCAL_PATH_LOCK_TTL_MS, 'NX');

  return {
    enabled: true,
    lockKey,
    lockToken,
    acquired: result === 'OK',
  };
}

// 释放本地路径锁（仅在 token 匹配时删除）。
export async function releaseRelayPathLock(params: {
  lockKey: string;
  lockToken: string | null;
}) {
  if (!params.lockToken) return;

  const lua = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
    end
    return 0
  `;

  await connection.eval(lua, 1, params.lockKey, params.lockToken);
}

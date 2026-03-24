import { createHash } from 'node:crypto';
import { connection } from '../infra/redis';
import { RELAY_LOCAL_PATH_LOCK_TTL_MS } from '../config/env';

function buildRelayPathLockKey(localPath: string) {
  const digest = createHash('sha1').update(localPath.toLowerCase()).digest('hex');
  return `lock:relay:path:${digest}`;
}

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

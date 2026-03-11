import { connection } from '../infra/redis';
import { TASK_DEFINITION_LOCK_TTL_MS } from '../config/env';
import { getTaskDefinitionLockKey as getLockKey } from '../schedule-utils';

export function getTaskDefinitionLockKey(taskDefinitionId: bigint) {
  return getLockKey(taskDefinitionId);
}

export async function tryAcquireTaskDefinitionLock(taskDefinitionId: bigint) {
  const lockToken = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const result = await connection.set(
    getTaskDefinitionLockKey(taskDefinitionId),
    lockToken,
    'PX',
    TASK_DEFINITION_LOCK_TTL_MS,
    'NX',
  );

  if (result !== 'OK') return null;
  return lockToken;
}

export async function releaseTaskDefinitionLock(taskDefinitionId: bigint, lockToken: string) {
  const lockKey = getTaskDefinitionLockKey(taskDefinitionId);
  const lua = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
    end
    return 0
  `;

  await connection.eval(lua, 1, lockKey, lockToken);
}

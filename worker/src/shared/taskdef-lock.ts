/**
 * 任务定义锁工具：供 scheduler/worker/service 共享使用。
 * 通过 Redis 分布式锁避免同一 task definition 并发执行。
 */

import { connection } from '../infra/redis';
import { TASK_DEFINITION_LOCK_TTL_MS } from '../config/env';
import { getTaskDefinitionLockKey as getLockKey } from '../schedule-utils';
import { logger } from '../logger';

// 获取任务定义锁 key。
export function getTaskDefinitionLockKey(taskDefinitionId: bigint) {
  return getLockKey(taskDefinitionId);
}

// 尝试获取任务定义锁。
export async function tryAcquireTaskDefinitionLock(taskDefinitionId: bigint) {
  const lockKey = getTaskDefinitionLockKey(taskDefinitionId);
  const lockToken = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const result = await connection.set(
    lockKey,
    lockToken,
    'PX',
    TASK_DEFINITION_LOCK_TTL_MS,
    'NX',
  );

  if (result !== 'OK') {
    try {
      const [currentToken, ttlMs] = await Promise.all([
        connection.get(lockKey),
        connection.pttl(lockKey),
      ]);
      logger.warn('[scheduler:taskdef] 锁被占用', {
        taskDefinitionId: taskDefinitionId.toString(),
        lockKey,
        holder: currentToken,
        ttlMs,
      });
    } catch (error) {
      logger.warn('[scheduler:taskdef] 锁被占用，且读取锁信息失败', {
        taskDefinitionId: taskDefinitionId.toString(),
        lockKey,
        error: error instanceof Error ? error.message : String(error),
      });
    }
    return null;
  }

  return lockToken;
}

// 释放任务定义锁（仅在 token 匹配时删除）。
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

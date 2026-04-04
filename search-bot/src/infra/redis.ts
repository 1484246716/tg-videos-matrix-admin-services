import Redis from 'ioredis';
import { env } from '../config/env';

export const redis = new Redis(env.REDIS_URL, {
  lazyConnect: true,
  maxRetriesPerRequest: 1,
  enableReadyCheck: true,
});

export async function checkRedisHealth() {
  try {
    if (redis.status !== 'ready') {
      await redis.connect();
    }
    const pong = await redis.ping();
    return pong === 'PONG';
  } catch {
    return false;
  }
}

export async function markUpdateIdempotent(updateId: number): Promise<boolean> {
  const key = `sb:idem:update:${updateId}`;
  const result = await redis.set(key, '1', 'EX', 120, 'NX');
  return result === 'OK';
}

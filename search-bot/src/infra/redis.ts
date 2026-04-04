import Redis from 'ioredis';
import { env } from '../config/env';

export const redis = new Redis(env.REDIS_URL, {
  lazyConnect: true,
  maxRetriesPerRequest: 1,
  enableReadyCheck: true,
});

async function ensureRedisReady() {
  if (redis.status !== 'ready') {
    await redis.connect();
  }
}

export async function checkRedisHealth() {
  try {
    await ensureRedisReady();
    const pong = await redis.ping();
    return pong === 'PONG';
  } catch {
    return false;
  }
}

export async function markUpdateIdempotent(updateId: number): Promise<boolean> {
  await ensureRedisReady();
  const key = `sb:idem:update:${updateId}`;
  const result = await redis.set(key, '1', 'EX', 120, 'NX');
  return result === 'OK';
}

export async function setJsonWithTtl(key: string, value: unknown, ttlSec: number): Promise<void> {
  await ensureRedisReady();
  await redis.set(key, JSON.stringify(value), 'EX', ttlSec);
}

export async function getJson<T>(key: string): Promise<T | null> {
  await ensureRedisReady();
  const raw = await redis.get(key);
  if (!raw) return null;
  return JSON.parse(raw) as T;
}

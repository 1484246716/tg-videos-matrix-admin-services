import { env } from '../../config/env';
import { redis } from '../../infra/redis';

function minuteBucket() {
  return Math.floor(Date.now() / 60000);
}

async function hitAndCheck(key: string, limit: number): Promise<boolean> {
  const count = await redis.incr(key);
  if (count === 1) {
    await redis.expire(key, 90);
  }
  return count <= limit;
}

export async function allowUserRequest(userId: string): Promise<boolean> {
  const key = `sb:rl:user:${userId}:${minuteBucket()}`;
  return hitAndCheck(key, env.SEARCH_BOT_USER_RATE_LIMIT);
}

export async function allowChannelRequest(channelId: string): Promise<boolean> {
  const key = `sb:rl:channel:${channelId}:${minuteBucket()}`;
  return hitAndCheck(key, env.SEARCH_BOT_CHANNEL_RATE_LIMIT);
}

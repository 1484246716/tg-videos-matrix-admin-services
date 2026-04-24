/**
 * Clone Channels guard-wait 公平性服务：按频道维度做轮询出队。
 * 用于在 clone 调度/执行链路中避免某个频道长期占满 guard-wait 处理机会。
 */

import { connection } from '../../infra/redis';

const RR_CURSOR_KEY = 'clone:guard_wait:rr:cursor';
const CH_QUEUE_KEY_PREFIX = 'clone:guard_wait:chq:';
const CH_SET_KEY = 'clone:guard_wait:channels';

// 规范化频道用户名：去除 @ 前缀并统一小写。
function normalizeChannelUsername(raw: string) {
  return raw.trim().replace(/^@+/, '').toLowerCase();
}

// 构造频道维度的 guard-wait 队列 key。
function buildChannelQueueKey(channelUsername: string) {
  return `${CH_QUEUE_KEY_PREFIX}${normalizeChannelUsername(channelUsername)}`;
}

// 按频道入队 guard-wait 任务。
export async function enqueueGuardWaitJobByChannel(params: {
  channelUsername: string;
  payload: unknown;
}) {
  const ch = normalizeChannelUsername(params.channelUsername);
  if (!ch) return;

  const qKey = buildChannelQueueKey(ch);
  await connection.sadd(CH_SET_KEY, ch);
  await connection.rpush(qKey, JSON.stringify(params.payload));
}

// 按轮询策略从各频道队列出队下一个 guard-wait 任务。
export async function dequeueNextGuardWaitJobRoundRobin() {
  const channels = await connection.smembers(CH_SET_KEY);
  if (!channels.length) {
    return null as null | { channelUsername: string; payload: Record<string, unknown>; remaining: number };
  }

  const cursorRaw = await connection.get(RR_CURSOR_KEY);
  const cursor = Number.isFinite(Number(cursorRaw)) ? Math.max(0, Number(cursorRaw)) : 0;

  for (let i = 0; i < channels.length; i += 1) {
    const idx = (cursor + i) % channels.length;
    const ch = channels[idx];
    const qKey = buildChannelQueueKey(ch);
    const item = await connection.lpop(qKey);
    if (!item) {
      const remain = await connection.llen(qKey);
      if (remain <= 0) await connection.srem(CH_SET_KEY, ch);
      continue;
    }

    const remaining = await connection.llen(qKey);
    if (remaining <= 0) {
      await connection.srem(CH_SET_KEY, ch);
    }

    await connection.set(RR_CURSOR_KEY, String((idx + 1) % Math.max(channels.length, 1)));

    return {
      channelUsername: ch,
      payload: JSON.parse(item) as Record<string, unknown>,
      remaining,
    };
  }

  await connection.set(RR_CURSOR_KEY, String((cursor + 1) % Math.max(channels.length, 1)));
  return null;
}

// 获取 guard-wait 公平性快照（频道数与队列深度 Top）。
export async function getGuardWaitFairnessSnapshot() {
  const channels = await connection.smembers(CH_SET_KEY);
  const top: Array<{ channelUsername: string; depth: number }> = [];

  for (const ch of channels.slice(0, 20)) {
    const depth = await connection.llen(buildChannelQueueKey(ch));
    top.push({ channelUsername: ch, depth });
  }

  top.sort((a, b) => b.depth - a.depth);

  return {
    channelCount: channels.length,
    topChannels: top.slice(0, 5),
  };
}

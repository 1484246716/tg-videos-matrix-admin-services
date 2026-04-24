/**
 * Clone Channels 通道公平性服务：控制同频道下载并发，避免热点频道长期占用。
 * 用于在 clone 调度/执行链路中做频道级互斥与陈旧锁自动接管。
 */

import { connection } from '../../infra/redis';

const CHANNEL_SLOT_TTL_MS = (() => {
  const n = Number(process.env.CLONE_DOWNLOAD_CHANNEL_SLOT_TTL_MS ?? `${45 * 60_000}`);
  if (!Number.isFinite(n) || n < 60_000) return 45 * 60_000;
  return Math.min(6 * 60 * 60_000, Math.floor(n));
})();

const CHANNEL_SLOT_STALE_MS = (() => {
  const n = Number(process.env.CLONE_DOWNLOAD_CHANNEL_SLOT_STALE_MS ?? '180000');
  if (!Number.isFinite(n) || n < 30_000) return 180000;
  return Math.min(60 * 60_000, Math.floor(n));
})();

// 规范化频道用户名：去除 @ 前缀并统一小写。
function normalizeChannelUsername(raw: string) {
  return raw.trim().replace(/^@+/, '').toLowerCase();
}

// 构造频道级下载槽位锁 key。
function buildChannelSlotKey(channelUsername: string) {
  return `lock:clone:download:channel:${normalizeChannelUsername(channelUsername)}`;
}

// 尝试获取频道槽位：支持对陈旧锁进行替换接管。
export async function tryAcquireCloneChannelSlot(channelUsername: string) {
  const key = buildChannelSlotKey(channelUsername);
  const token = `${Date.now()}-${Math.random().toString(16).slice(2)}`;

  const lua = `
    local key = KEYS[1]
    local token = ARGV[1]
    local ttl = tonumber(ARGV[2])
    local staleMs = tonumber(ARGV[3])
    local nowMs = tonumber(ARGV[4])

    local existing = redis.call("GET", key)
    if not existing then
      redis.call("SET", key, token, "PX", ttl, "NX")
      return "acquired"
    end

    local dashPos = string.find(existing, "-")
    local tsRaw = existing
    if dashPos then
      tsRaw = string.sub(existing, 1, dashPos - 1)
    end

    local ts = tonumber(tsRaw)
    local age = nil
    if ts then
      age = nowMs - ts
    end

    if age and age > staleMs then
      redis.call("SET", key, token, "PX", ttl)
      return "acquired_stale_replaced"
    end

    return "busy"
  `;

  const result = String(
    await connection.eval(lua, 1, key, token, CHANNEL_SLOT_TTL_MS, CHANNEL_SLOT_STALE_MS, Date.now()),
  );

  return {
    key,
    token,
    acquired: result === 'acquired' || result === 'acquired_stale_replaced',
    staleReplaced: result === 'acquired_stale_replaced',
  };
}

// 释放频道槽位：仅在 token 匹配时删除，避免误删他人锁。
export async function releaseCloneChannelSlot(params: {
  key: string;
  token: string;
}) {
  const lua = `
    if redis.call("GET", KEYS[1]) == ARGV[1] then
      return redis.call("DEL", KEYS[1])
    end
    return 0
  `;

  await connection.eval(lua, 1, params.key, params.token);
}

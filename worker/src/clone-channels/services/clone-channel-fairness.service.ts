import { connection } from '../../infra/redis';

const CHANNEL_SLOT_TTL_MS = (() => {
  const n = Number(process.env.CLONE_DOWNLOAD_CHANNEL_SLOT_TTL_MS ?? `${45 * 60_000}`);
  if (!Number.isFinite(n) || n < 60_000) return 45 * 60_000;
  return Math.min(6 * 60 * 60_000, Math.floor(n));
})();

function normalizeChannelUsername(raw: string) {
  return raw.trim().replace(/^@+/, '').toLowerCase();
}

function buildChannelSlotKey(channelUsername: string) {
  return `lock:clone:download:channel:${normalizeChannelUsername(channelUsername)}`;
}

export async function tryAcquireCloneChannelSlot(channelUsername: string) {
  const key = buildChannelSlotKey(channelUsername);
  const token = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const result = await connection.set(key, token, 'PX', CHANNEL_SLOT_TTL_MS, 'NX');

  return {
    key,
    token,
    acquired: result === 'OK',
  };
}

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

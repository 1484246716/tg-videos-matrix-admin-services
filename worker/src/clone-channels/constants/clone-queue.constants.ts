/**
 * Clone Channels 队列与运行时参数常量定义。
 * 统一提供 clone 相关 scheduler/worker/service 所依赖的配置项与默认值。
 */

export const CLONE_QUEUE_NAMES = {
  crawlSchedule: 'q_clone_crawl_schedule',
  channelIndex: 'q_clone_channel_index',
  videoDownload: 'q_clone_video_download',
  groupL1Dispatch: 'q_clone_group_l1_dispatch',
  groupL2Download: 'q_clone_group_l2_download',
  retry: 'q_clone_retry',
} as const;

export const CLONE_DOWNLOAD_GLOBAL_CONCURRENCY = (() => {
  const n = Number(process.env.CLONE_DOWNLOAD_GLOBAL_CONCURRENCY ?? '1');
  if (!Number.isFinite(n) || n < 1) return 1;
  return Math.min(32, Math.floor(n));
})();

export const CLONE_DOWNLOAD_CHANNEL_CONCURRENCY = (() => {
  const n = Number(process.env.CLONE_DOWNLOAD_CHANNEL_CONCURRENCY ?? '1');
  if (!Number.isFinite(n) || n < 1) return 1;
  return Math.min(8, Math.floor(n));
})();

export const CLONE_DISK_FUSE_THRESHOLD = (() => {
  const n = Number(process.env.CLONE_DISK_FUSE_THRESHOLD ?? '90');
  if (!Number.isFinite(n) || n < 50) return 90;
  return Math.min(99, Math.floor(n));
})();

export const CLONE_RETRY_MAX = (() => {
  const n = Number(process.env.CLONE_RETRY_MAX ?? '5');
  if (!Number.isFinite(n) || n < 0) return 5;
  return Math.min(20, Math.floor(n));
})();

export const CLONE_DOWNLOAD_CHUNK_SIZE_BYTES = (() => {
  const n = Number(process.env.CLONE_DOWNLOAD_CHUNK_SIZE_BYTES ?? `${4 * 1024 * 1024}`);
  if (!Number.isFinite(n) || n < 256 * 1024) return 4 * 1024 * 1024;
  return Math.min(32 * 1024 * 1024, Math.floor(n));
})();

export const CLONE_DOWNLOAD_TIMEOUT_MS = (() => {
  const n = Number(process.env.CLONE_DOWNLOAD_TIMEOUT_MS ?? '1200000');
  if (!Number.isFinite(n) || n < 10_000) return 1200000;
  return Math.min(30 * 60_000, Math.floor(n));
})();

export const CLONE_DOWNLOAD_VALIDATE_FFPROBE =
  process.env.CLONE_DOWNLOAD_VALIDATE_FFPROBE === 'true';

export const CLONE_DOWNLOAD_STRICT_VIDEO_ONLY =
  process.env.CLONE_DOWNLOAD_STRICT_VIDEO_ONLY !== 'false';

export const CLONE_DOWNLOAD_ALLOW_OPAQUE_FALLBACK =
  process.env.CLONE_DOWNLOAD_ALLOW_OPAQUE_FALLBACK === 'true';

export const CLONE_RETRY_BASE_DELAY_MS = (() => {
  const n = Number(process.env.CLONE_RETRY_BASE_DELAY_MS ?? '1000');
  if (!Number.isFinite(n) || n < 100) return 1000;
  return Math.min(60_000, Math.floor(n));
})();

export const CLONE_RETRY_MAX_DELAY_MS = (() => {
  const n = Number(process.env.CLONE_RETRY_MAX_DELAY_MS ?? '60000');
  if (!Number.isFinite(n) || n < 1000) return 60000;
  return Math.min(60 * 60_000, Math.floor(n));
})();

export const CLONE_GROUP_L1L2_ENABLED =
  process.env.CLONE_GROUP_L1L2_ENABLED !== 'false';

export const CLONE_GROUP_L1_GLOBAL_CONCURRENCY = (() => {
  const n = Number(process.env.CLONE_GROUP_L1_GLOBAL_CONCURRENCY ?? '8');
  if (!Number.isFinite(n) || n < 1) return 8;
  return Math.min(64, Math.floor(n));
})();

export const CLONE_GROUP_L2_PER_GROUP_CONCURRENCY = (() => {
  const n = Number(process.env.CLONE_GROUP_L2_PER_GROUP_CONCURRENCY ?? '1');
  if (!Number.isFinite(n) || n < 1) return 1;
  return Math.min(8, Math.floor(n));
})();

export const CLONE_GROUP_DISPATCH_TICK_MS = (() => {
  const n = Number(process.env.CLONE_GROUP_DISPATCH_TICK_MS ?? '200');
  if (!Number.isFinite(n) || n < 10) return 200;
  return Math.min(10_000, Math.floor(n));
})();

export const CLONE_GROUP_ASSEMBLE_TIMEOUT_MS = (() => {
  const n = Number(process.env.CLONE_GROUP_ASSEMBLE_TIMEOUT_MS ?? '600000');
  if (!Number.isFinite(n) || n < 60_000) return 600000;
  return Math.min(60 * 60_000, Math.floor(n));
})();

export const CLONE_USE_LUA_ATOMIC =
  process.env.CLONE_USE_LUA_ATOMIC !== 'false';

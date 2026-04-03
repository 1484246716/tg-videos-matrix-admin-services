import dotenv from 'dotenv';

// 加载环境变量：优先读取 apps/.env（相对 worker），再读取默认 .env
dotenv.config({ path: '../.env' });
dotenv.config();

/** Redis 连接地址 */
export const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';

/** Telegram Bot API 基础地址（可指向本地代理） */
export const telegramApiBase =
  process.env.TELEGRAM_BOT_API_BASE || 'http://localhost:8081';

/** 调度器轮询间隔（毫秒），最小 1000ms，默认 10000ms */
export const SCHEDULER_POLL_MS = (() => {
  const n = Number(process.env.SCHEDULER_POLL_MS ?? '10000');
  if (!Number.isFinite(n) || n < 1000) return 10000;
  return Math.floor(n);
})();

/** 单次调度扫描最大任务数 */
export const MAX_SCHEDULE_BATCH = 100;

/** 任务定义锁 TTL（毫秒） */
export const TASK_DEFINITION_LOCK_TTL_MS = Number(
  process.env.TASK_DEFINITION_LOCK_TTL_MS || '15000',
);

/** 任务定义失败后的重试等待（秒） */
export const TASK_DEFINITION_ERROR_RETRY_SEC = Number(
  process.env.TASK_DEFINITION_ERROR_RETRY_SEC || '300',
);

/** 是否启用分发任务（TypeB/Dispatch）频道级时间窗口保护 */
export const DISPATCH_CHANNEL_INTERVAL_GUARD_ENABLED = true;

/** 是否启用目录任务（TypeC/Catalog）频道级时间窗口保护 */
export const CATALOG_CHANNEL_INTERVAL_GUARD_ENABLED = true;

/** TypeC 自愈总开关（false 关闭） */
export const TYPEC_SELF_HEAL_ENABLED =
  process.env.TYPEC_SELF_HEAL_ENABLED !== 'false';

/** TypeC 在“未到执行窗口”时是否仍执行自愈检查（false 关闭） */
export const TYPEC_SELF_HEAL_ON_SKIP =
  process.env.TYPEC_SELF_HEAL_ON_SKIP !== 'false';

/** TypeC 在常规执行流程中是否启用自愈修复（false 关闭） */
export const TYPEC_SELF_HEAL_ON_RUN =
  process.env.TYPEC_SELF_HEAL_ON_RUN !== 'false';

/** TypeC 自愈时是否允许执行孤儿消息清理（false 关闭） */
export const TYPEC_SELF_HEAL_CLEANUP_ENABLED =
  process.env.TYPEC_SELF_HEAL_CLEANUP_ENABLED !== 'false';

/** TypeC 合集索引是否展示空合集（默认 true） */
export const TYPEC_COLLECTION_INDEX_SHOW_EMPTY =
  process.env.TYPEC_COLLECTION_INDEX_SHOW_EMPTY !== 'false';

/** TypeC 合集全量扫描批次大小（默认 1000） */
export const TYPEC_COLLECTION_FULL_SCAN_BATCH_SIZE = (() => {
  const n = Number(process.env.TYPEC_COLLECTION_FULL_SCAN_BATCH_SIZE ?? '1000');
  if (!Number.isFinite(n) || n < 100) return 1000;
  return Math.min(5000, Math.floor(n));
})();

/** TypeC 合集数据源（recent/full/cache，默认 full） */
export const TYPEC_COLLECTION_DATA_SOURCE =
  process.env.TYPEC_COLLECTION_DATA_SOURCE === 'cache'
    ? 'cache'
    : process.env.TYPEC_COLLECTION_DATA_SOURCE === 'recent'
      ? 'recent'
      : 'full';

/** TypeC 合集缓存过期阈值（秒） */
export const TYPEC_COLLECTION_CACHE_STALE_SECONDS = (() => {
  const n = Number(process.env.TYPEC_COLLECTION_CACHE_STALE_SECONDS ?? '300');
  if (!Number.isFinite(n) || n < 30) return 300;
  return Math.floor(n);
})();

/** TypeC 合集缓存不可用时是否回源DB */
export const TYPEC_COLLECTION_CACHE_FALLBACK_TO_DB =
  process.env.TYPEC_COLLECTION_CACHE_FALLBACK_TO_DB !== 'false';

/** 合集快照增量刷新批次大小 */
export const COLLECTION_SNAPSHOT_INCREMENTAL_BATCH_SIZE = (() => {
  const n = Number(process.env.COLLECTION_SNAPSHOT_INCREMENTAL_BATCH_SIZE ?? '1000');
  if (!Number.isFinite(n) || n < 100) return 1000;
  return Math.min(5000, Math.floor(n));
})();

/** 是否启用频道级分布式锁 */
export const CHANNEL_LOCK_ENABLED = true;

/** 频道级锁 TTL（毫秒） */
export const CHANNEL_LOCK_TTL_MS = Number(process.env.CHANNEL_LOCK_TTL_MS || '60000');

/** GramJS API ID */
export const GRAMJS_API_ID = Number(process.env.GRAMJS_API_ID || '0');

/** GramJS API HASH */
export const GRAMJS_API_HASH = process.env.GRAMJS_API_HASH || '';

/** GramJS Bot Token */
export const GRAMJS_BOT_TOKEN = process.env.GRAMJS_BOT_TOKEN || '';

/** GramJS 会话串 */
export const GRAMJS_SESSION = process.env.GRAMJS_SESSION || '';

/** GramJS 转发目标聊天 ID */
export const GRAMJS_FORWARD_TARGET_CHAT_ID =
  process.env.GRAMJS_FORWARD_TARGET_CHAT_ID || '';

/** GramJS 上传并发 worker 数 */
export const GRAMJS_UPLOAD_WORKERS = Number(
  process.env.GRAMJS_UPLOAD_WORKERS || '16',
);

/** 超过该大小（MB）优先走 GramJS 上传 */
export const RELAY_UPLOAD_GRAMJS_THRESHOLD_MB = Number(
  process.env.RELAY_UPLOAD_GRAMJS_THRESHOLD_MB || '1024',
);

/** 超过该大小（MB）时发送为 document 而非普通 video */
export const RELAY_UPLOAD_SEND_DOCUMENT_THRESHOLD_MB = Number(
  process.env.RELAY_UPLOAD_SEND_DOCUMENT_THRESHOLD_MB || '900',
);

/** BullMQ q_relay_upload 并发（同时处理几条中转上传任务），默认 1，上限 32 */
export const RELAY_UPLOAD_QUEUE_CONCURRENCY = (() => {
  const n = Number(process.env.RELAY_UPLOAD_QUEUE_CONCURRENCY ?? '1');
  if (!Number.isFinite(n) || n < 1) return 1;
  return Math.min(32, Math.floor(n));
})();

/** 上传进度日志里程碑（百分比，逗号分隔），默认 20,50,100 */
export const RELAY_UPLOAD_PROGRESS_LOG_MILESTONES = (() => {
  const raw = process.env.RELAY_UPLOAD_PROGRESS_LOG_MILESTONES ?? '20,50,100';
  const parsed = raw
    .split(',')
    .map((s) => Number(s.trim()))
    .filter((n) => Number.isFinite(n) && n > 0 && n <= 100)
    .map((n) => Math.floor(n));

  const uniqueSorted = Array.from(new Set(parsed)).sort((a, b) => a - b);

  if (uniqueSorted.length === 0) return [20, 50, 100];
  if (!uniqueSorted.includes(100)) uniqueSorted.push(100);

  return uniqueSorted;
})();

/** 中转文件“稳定检测”最少连续通过次数 */
export const RELAY_MIN_STABLE_CHECKS = Number(
  process.env.RELAY_MIN_STABLE_CHECKS || '3',
);

/** 中转文件稳定检测轮询间隔（毫秒） */
export const RELAY_STABLE_INTERVAL_MS = Number(
  process.env.RELAY_STABLE_INTERVAL_MS || '10000',
);

/** 文件 mtime 冷却时间（毫秒），用于避免刚写完即读取 */
export const RELAY_MTIME_COOLDOWN_MS = Number(
  process.env.RELAY_MTIME_COOLDOWN_MS || '120000',
);

// ===== FFprobe 配置 =====
/** 是否启用 FFprobe 完整性检查 */
export const RELAY_ENABLE_FFPROBE_CHECK = process.env.RELAY_ENABLE_FFPROBE_CHECK === 'true';

/** FFprobe 超时时间（毫秒） */
export const RELAY_FFPROBE_TIMEOUT_MS = parseInt(process.env.RELAY_FFPROBE_TIMEOUT_MS || '15000', 10);

/** FFprobe 判定最小时长（秒） */
export const RELAY_FFPROBE_MIN_DURATION_SEC = parseInt(process.env.RELAY_FFPROBE_MIN_DURATION_SEC || '1', 10);

/** 本地路径锁 TTL（毫秒） */
export const RELAY_LOCAL_PATH_LOCK_TTL_MS = Number(
  process.env.RELAY_LOCAL_PATH_LOCK_TTL_MS || '120000',
);

/** TypeA 入库最大重试次数 */
export const TYPEA_INGEST_MAX_RETRIES = Number(
  process.env.TYPEA_INGEST_MAX_RETRIES || '3',
);

/** TypeA 入库任务过期阈值（毫秒） */
export const TYPEA_INGEST_STALE_MS = Number(
  process.env.TYPEA_INGEST_STALE_MS || '1800000',
);

/** TypeA 对账单批次大小 */
export const TYPEA_RECONCILE_BATCH = Number(
  process.env.TYPEA_RECONCILE_BATCH || '200',
);

/** TypeA 入库租约时长（毫秒） */
export const TYPEA_INGEST_LEASE_MS = Number(
  process.env.TYPEA_INGEST_LEASE_MS || '900000',
);

/** TypeA 是否启用对账修复 */
export const TYPEA_RECONCILE_ENABLED =
  process.env.TYPEA_RECONCILE_ENABLED !== 'false';

/** TypeA 文件缺失是否直接失败 */
export const TYPEA_FAIL_ON_FILE_MISSING =
  process.env.TYPEA_FAIL_ON_FILE_MISSING !== 'false';

/** TypeA 允许上传的最大文件大小（MB） */
export const TYPEA_MAX_UPLOAD_SIZE_MB = Number(
  process.env.TYPEA_MAX_UPLOAD_SIZE_MB || '2048',
);

/** TypeA 告警：任务陈旧阈值 */
export const TYPEA_ALERT_STALE_THRESHOLD = Number(
  process.env.TYPEA_ALERT_STALE_THRESHOLD || '0',
);

/** TypeA 告警：最终失败突增阈值 */
export const TYPEA_ALERT_FAILED_FINAL_SPIKE_THRESHOLD = Number(
  process.env.TYPEA_ALERT_FAILED_FINAL_SPIKE_THRESHOLD || '5',
);

/** TypeA 告警：队列卡住分钟阈值 */
export const TYPEA_ALERT_QUEUE_STUCK_MINUTES = Number(
  process.env.TYPEA_ALERT_QUEUE_STUCK_MINUTES || '5',
);

/** Telegram 429/暂时错误重试最大次数（含首发） */
export const TG_RETRY_MAX_ATTEMPTS = (() => {
  const n = Number(process.env.TG_RETRY_MAX_ATTEMPTS ?? '5');
  if (!Number.isFinite(n) || n < 1) return 5;
  return Math.min(10, Math.floor(n));
})();

/** Telegram 重试最大退避秒数 */
export const TG_RETRY_BACKOFF_MAX_SECONDS = (() => {
  const n = Number(process.env.TG_RETRY_BACKOFF_MAX_SECONDS ?? '60');
  if (!Number.isFinite(n) || n < 1) return 60;
  return Math.min(300, Math.floor(n));
})();

/** Telegram 请求最小间隔（毫秒，全局） */
export const TG_SEND_MIN_INTERVAL_MS = (() => {
  const n = Number(process.env.TG_SEND_MIN_INTERVAL_MS ?? '120');
  if (!Number.isFinite(n) || n < 0) return 120;
  return Math.min(5000, Math.floor(n));
})();

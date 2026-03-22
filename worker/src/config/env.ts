import dotenv from 'dotenv';

dotenv.config({ path: '../.env' });
dotenv.config();

export const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
export const telegramApiBase =
  process.env.TELEGRAM_BOT_API_BASE || 'http://localhost:8081';

export const SCHEDULER_POLL_MS = 25000;
export const MAX_SCHEDULE_BATCH = 100;
export const TASK_DEFINITION_LOCK_TTL_MS = Number(
  process.env.TASK_DEFINITION_LOCK_TTL_MS || '15000',
);
export const TASK_DEFINITION_ERROR_RETRY_SEC = Number(
  process.env.TASK_DEFINITION_ERROR_RETRY_SEC || '300',
);

export const DISPATCH_CHANNEL_INTERVAL_GUARD_ENABLED = true;
export const CATALOG_CHANNEL_INTERVAL_GUARD_ENABLED = true;
export const CHANNEL_LOCK_ENABLED = true;
export const CHANNEL_LOCK_TTL_MS = Number(process.env.CHANNEL_LOCK_TTL_MS || '60000');

export const GRAMJS_API_ID = Number(process.env.GRAMJS_API_ID || '0');
export const GRAMJS_API_HASH = process.env.GRAMJS_API_HASH || '';
export const GRAMJS_BOT_TOKEN = process.env.GRAMJS_BOT_TOKEN || '';
export const GRAMJS_SESSION = process.env.GRAMJS_SESSION || '';
export const GRAMJS_FORWARD_TARGET_CHAT_ID =
  process.env.GRAMJS_FORWARD_TARGET_CHAT_ID || '';
export const GRAMJS_UPLOAD_WORKERS = Number(
  process.env.GRAMJS_UPLOAD_WORKERS || '16',
);
export const RELAY_UPLOAD_GRAMJS_THRESHOLD_MB = Number(
  process.env.RELAY_UPLOAD_GRAMJS_THRESHOLD_MB || '1024',
);
export const RELAY_UPLOAD_SEND_DOCUMENT_THRESHOLD_MB = Number(
  process.env.RELAY_UPLOAD_SEND_DOCUMENT_THRESHOLD_MB || '900',
);

export const RELAY_MIN_STABLE_CHECKS = Number(
  process.env.RELAY_MIN_STABLE_CHECKS || '3',
);
export const RELAY_STABLE_INTERVAL_MS = Number(
  process.env.RELAY_STABLE_INTERVAL_MS || '10000',
);
export const RELAY_MTIME_COOLDOWN_MS = Number(
  process.env.RELAY_MTIME_COOLDOWN_MS || '120000',
);

// ===== FFprobe 配置 =====
export const RELAY_ENABLE_FFPROBE_CHECK = process.env.RELAY_ENABLE_FFPROBE_CHECK === 'true';
export const RELAY_FFPROBE_TIMEOUT_MS = parseInt(process.env.RELAY_FFPROBE_TIMEOUT_MS || '15000', 10);
export const RELAY_FFPROBE_MIN_DURATION_SEC = parseInt(process.env.RELAY_FFPROBE_MIN_DURATION_SEC || '1', 10);

export const TYPEA_INGEST_MAX_RETRIES = Number(
  process.env.TYPEA_INGEST_MAX_RETRIES || '3',
);
export const TYPEA_INGEST_STALE_MS = Number(
  process.env.TYPEA_INGEST_STALE_MS || '1800000',
);
export const TYPEA_RECONCILE_BATCH = Number(
  process.env.TYPEA_RECONCILE_BATCH || '200',
);
export const TYPEA_INGEST_LEASE_MS = Number(
  process.env.TYPEA_INGEST_LEASE_MS || '900000',
);
export const TYPEA_RECONCILE_ENABLED =
  process.env.TYPEA_RECONCILE_ENABLED !== 'false';
export const TYPEA_FAIL_ON_FILE_MISSING =
  process.env.TYPEA_FAIL_ON_FILE_MISSING !== 'false';
export const TYPEA_MAX_UPLOAD_SIZE_MB = Number(
  process.env.TYPEA_MAX_UPLOAD_SIZE_MB || '2048',
);

export const TYPEA_ALERT_STALE_THRESHOLD = Number(
  process.env.TYPEA_ALERT_STALE_THRESHOLD || '0',
);
export const TYPEA_ALERT_FAILED_FINAL_SPIKE_THRESHOLD = Number(
  process.env.TYPEA_ALERT_FAILED_FINAL_SPIKE_THRESHOLD || '5',
);
export const TYPEA_ALERT_QUEUE_STUCK_MINUTES = Number(
  process.env.TYPEA_ALERT_QUEUE_STUCK_MINUTES || '5',
);
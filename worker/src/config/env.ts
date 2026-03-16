import dotenv from 'dotenv';

dotenv.config({ path: '../.env' });
dotenv.config();

export const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
export const telegramApiBase =
  process.env.TELEGRAM_BOT_API_BASE || 'http://localhost:8081';

export const SCHEDULER_POLL_MS = 5000;
export const MAX_SCHEDULE_BATCH = 100;
export const TASK_DEFINITION_LOCK_TTL_MS = Number(
  process.env.TASK_DEFINITION_LOCK_TTL_MS || '15000',
);
export const TASK_DEFINITION_ERROR_RETRY_SEC = Number(
  process.env.TASK_DEFINITION_ERROR_RETRY_SEC || '300',
);

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

export const RELAY_MIN_STABLE_CHECKS = Number(
  process.env.RELAY_MIN_STABLE_CHECKS || '3',
);
export const RELAY_STABLE_INTERVAL_MS = Number(
  process.env.RELAY_STABLE_INTERVAL_MS || '10000',
);
export const RELAY_MTIME_COOLDOWN_MS = Number(
  process.env.RELAY_MTIME_COOLDOWN_MS || '60000',
);

import dotenv from 'dotenv';

dotenv.config({ path: '../.env' });
dotenv.config();

export const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
export const telegramApiBase =
  process.env.TELEGRAM_BOT_API_BASE || 'http://localhost:8081';

export const SCHEDULER_POLL_MS = 5000;
export const MAX_SCHEDULE_BATCH = 100;
export const TASK_DEFINITION_LOCK_TTL_MS = Number(
  process.env.TASK_DEFINITION_LOCK_TTL_MS || '3600000',
);
export const TASK_DEFINITION_ERROR_RETRY_SEC = Number(
  process.env.TASK_DEFINITION_ERROR_RETRY_SEC || '300',
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

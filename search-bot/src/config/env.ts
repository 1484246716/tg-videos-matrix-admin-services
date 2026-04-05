import dotenv from 'dotenv';
import { z } from 'zod';

dotenv.config();

const EnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  SEARCH_BOT_PORT: z.coerce.number().int().positive().default(3310),
  BOT_TOKEN: z.string().min(1).default('dev-bot-token'),
  BOT_WEBHOOK_SECRET: z.string().min(1).default('dev-webhook-secret'),
  API_BASE_URL: z.string().url().default('http://localhost:3000'),
  API_INTERNAL_TOKEN: z.string().min(1).default('dev-internal-token'),
  REDIS_URL: z.string().min(1).default('redis://127.0.0.1:6379'),
  SEARCH_BOT_CALLBACK_TTL_SEC: z.coerce.number().int().min(60).max(3600).default(180),
  SEARCH_BOT_USER_RATE_LIMIT: z.coerce.number().int().min(1).default(10),
  SEARCH_BOT_CHANNEL_RATE_LIMIT: z.coerce.number().int().min(1).default(200),
  SEARCH_BOT_COPY_RETRY_MAX: z.coerce.number().int().min(0).max(5).default(1),
  SEARCH_BOT_COPY_RETRY_BACKOFF_MS: z.coerce.number().int().min(50).max(5000).default(200),
  SEARCH_BOT_COPY_IDEMPOTENT_TTL_SEC: z.coerce.number().int().min(60).max(172800).default(86400),
  SEARCH_BOT_BOT_USERNAME: z.string().min(1).default('dev_search_bot'),
  SEARCH_BOT_DEEPLINK_TTL_SEC: z.coerce.number().int().min(60).max(3600).default(600),
});

export type AppEnv = z.infer<typeof EnvSchema>;

export const env: AppEnv = EnvSchema.parse(process.env);

import dotenv from 'dotenv';
import { z } from 'zod';

dotenv.config();

const EnvSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  MTPROTO_EXECUTOR_PORT: z.coerce.number().int().positive().default(3320),
  MTPROTO_EXECUTOR_INTERNAL_TOKEN: z.string().min(1).default('dev-mtproto-internal-token'),
  MTPROTO_API_ID: z.string().min(1).default('dev-api-id'),
  MTPROTO_API_HASH: z.string().min(1).default('dev-api-hash'),
  MTPROTO_STRING_SESSION: z.string().min(1).default('dev-string-session'),
  MTPROTO_DRY_RUN: z.coerce.boolean().default(true),
});

export type MtprotoEnv = z.infer<typeof EnvSchema>;

export const env: MtprotoEnv = EnvSchema.parse(process.env);

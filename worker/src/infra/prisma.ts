import { PrismaClient } from '@prisma/client';
import crypto from 'crypto';
import { logger } from '../logger';

export const prisma = new PrismaClient();

function assertModelAvailable(model: unknown, modelName: string) {
  if (!model) {
    throw new Error(
      `Prisma model '${modelName}' is unavailable in worker runtime. ` +
        "Please run 'pnpm run prisma:generate' in apps/worker and restart worker.",
    );
  }
}

function getDatabaseInfoFromUrl(rawUrl: string | undefined) {
  if (!rawUrl) {
    return {
      hasDatabaseUrl: false,
      dbFingerprint: null,
      dbProtocol: null,
      dbHost: null,
      dbPort: null,
      dbName: null,
    };
  }

  const dbFingerprint = crypto.createHash('sha256').update(rawUrl).digest('hex').slice(0, 12);

  try {
    const parsed = new URL(rawUrl);
    const dbName = parsed.pathname.replace(/^\//, '') || null;
    return {
      hasDatabaseUrl: true,
      dbFingerprint,
      dbProtocol: parsed.protocol.replace(':', ''),
      dbHost: parsed.hostname || null,
      dbPort: parsed.port || null,
      dbName,
    };
  } catch {
    return {
      hasDatabaseUrl: true,
      dbFingerprint,
      dbProtocol: 'unparsed',
      dbHost: null,
      dbPort: null,
      dbName: null,
    };
  }
}

export function logWorkerDatabaseFingerprint() {
  const info = getDatabaseInfoFromUrl(process.env.DATABASE_URL);
  logger.info('[bootstrap] Worker 数据库连接信息(脱敏)', info);
}

export function ensureWorkerPrismaModels() {
  assertModelAvailable((prisma as any).taskDefinition, 'taskDefinition');

  if (!(prisma as any).collection) {
    logger.warn('[bootstrap] Prisma collection 模型缺失，合集配置将退回频道级分页');
  }
}

export function getTaskDefinitionModel() {
  const model = prisma.taskDefinition;
  assertModelAvailable(model, 'taskDefinition');
  return model;
}

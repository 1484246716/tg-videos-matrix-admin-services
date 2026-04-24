/**
 * Prisma 基础设施封装：提供连接、重试、模型可用性检查与诊断日志。
 * 为 bootstrap / services / schedulers 提供统一数据库访问入口。
 */

import { PrismaClient } from '@prisma/client';
import crypto from 'crypto';
import { logger } from '../logger';

export const prisma = new PrismaClient();

// 简单延迟函数，用于重试退避。
function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// 判断是否属于 Prisma 可重试的连接类错误。
export function isPrismaRetryableConnectionError(error: unknown) {
  if (!error || typeof error !== 'object') return false;
  const maybe = error as { code?: string; message?: string; name?: string };
  const code = maybe.code ?? '';
  const message = maybe.message ?? '';
  return code === 'P1017' || /Server has closed the connection/i.test(message);
}

// 为数据库操作提供带指数退避的重试封装。
export async function withPrismaRetry<T>(
  action: () => Promise<T>,
  options?: { maxAttempts?: number; baseDelayMs?: number; label?: string },
): Promise<T> {
  const maxAttempts = Math.max(1, Math.floor(options?.maxAttempts ?? 3));
  const baseDelayMs = Math.max(100, Math.floor(options?.baseDelayMs ?? 1000));

  let lastError: unknown;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      return await action();
    } catch (error) {
      lastError = error;
      if (!isPrismaRetryableConnectionError(error) || attempt >= maxAttempts) {
        throw error;
      }

      const delayMs = Math.min(4000, baseDelayMs * 2 ** (attempt - 1));
      logger.warn('[prisma_retry] 命中可重试数据库连接错误，准备重试', {
        label: options?.label ?? null,
        attempt,
        maxAttempts,
        delayMs,
        reason: error instanceof Error ? error.message : String(error),
      });
      await sleep(delayMs);
    }
  }

  throw lastError instanceof Error ? lastError : new Error('未知 Prisma 重试错误');
}

// 断言模型在当前 worker 运行时可用。
function assertModelAvailable(model: unknown, modelName: string) {
  if (!model) {
    throw new Error(
      `Prisma model '${modelName}' is unavailable in worker runtime. ` +
        "Please run 'pnpm run prisma:generate' in apps/worker and restart worker.",
    );
  }
}

// 从 DATABASE_URL 提取脱敏后的连接信息。
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

// 记录 worker 侧数据库连接指纹（脱敏）。
export function logWorkerDatabaseFingerprint() {
  const info = getDatabaseInfoFromUrl(process.env.DATABASE_URL);
  logger.info('[bootstrap] Worker 数据库连接信息(脱敏)', info);
}

// 启动时校验 worker 所需 Prisma 模型是否可用。
export function ensureWorkerPrismaModels() {
  assertModelAvailable((prisma as any).taskDefinition, 'taskDefinition');

  if (!(prisma as any).collection) {
    logger.warn('[bootstrap] Prisma collection 模型缺失，合集配置将退回频道级分页');
  }

  if (!(prisma as any).searchDocument) {
    logger.warn('[bootstrap] Prisma searchDocument 模型缺失，搜索索引功能不可用。请运行 pnpm run prisma:generate');
  }
}

// 获取并断言 taskDefinition 模型可用。
export function getTaskDefinitionModel() {
  const model = prisma.taskDefinition;
  assertModelAvailable(model, 'taskDefinition');
  return model;
}

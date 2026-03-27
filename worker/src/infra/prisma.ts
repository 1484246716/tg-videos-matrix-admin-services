import { PrismaClient } from '@prisma/client';
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

import { PrismaClient } from '@prisma/client';

export const prisma = new PrismaClient();

export function getTaskDefinitionModel() {
  const model = prisma.taskDefinition;
  if (!model) {
    throw new Error(
      'Prisma taskDefinition model is unavailable. Please run prisma generate and restart worker.',
    );
  }

  return model;
}

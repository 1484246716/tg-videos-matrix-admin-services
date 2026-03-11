import { getTaskDefinitionModel } from '../infra/prisma';

export async function updateTaskDefinitionRunStatus(args: {
  taskDefinitionId: bigint;
  status: 'success' | 'failed';
  summary: Record<string, unknown>;
}) {
  await getTaskDefinitionModel().update({
    where: { id: args.taskDefinitionId },
    data: {
      lastRunAt: new Date(),
      lastRunStatus: args.status,
      lastRunSummary: args.summary as any,
    },
  });
}

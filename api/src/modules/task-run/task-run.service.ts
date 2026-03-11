import { Injectable, NotFoundException } from '@nestjs/common';
import { TaskDefinitionType, TaskRunStatus, TaskRunStepStatus, TaskRunEntityType } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class TaskRunService {
  constructor(private readonly prisma: PrismaService) {}

  async list(params: {
    taskDefinitionId?: string;
    taskType?: TaskDefinitionType;
    status?: TaskRunStatus;
    limit?: number;
  }) {
    const runs = await this.prisma.taskRun.findMany({
      where: {
        taskDefinitionId: params.taskDefinitionId
          ? BigInt(params.taskDefinitionId)
          : undefined,
        taskType: params.taskType,
        status: params.status,
      },
      orderBy: { createdAt: 'desc' },
      take: params.limit ?? 50,
      include: {
        taskDefinition: {
          select: {
            id: true,
            name: true,
          },
        },
      },
    });

    if (runs.length === 0) return [];

    const runIds = runs.map((run) => run.id);
    const steps = await this.prisma.taskRunStep.findMany({
      where: {
        taskRunId: { in: runIds },
      },
      orderBy: { createdAt: 'desc' },
      take: 500,
      select: {
        taskRunId: true,
        payload: true,
      },
    });

    const stepMetaMap = new Map<string, { channelName?: string | null; mediaName?: string | null }>();
    for (const step of steps) {
      const runId = step.taskRunId.toString();
      if (stepMetaMap.has(runId)) continue;
      const payload = (step.payload ?? {}) as Record<string, unknown>;
      const channelName = payload.channelName;
      const mediaName = payload.mediaName;
      if (typeof channelName === 'string' || typeof mediaName === 'string') {
        stepMetaMap.set(runId, {
          channelName: typeof channelName === 'string' ? channelName : null,
          mediaName: typeof mediaName === 'string' ? mediaName : null,
        });
      }
    }

    return runs.map((run) => ({
      ...run,
      id: run.id.toString(),
      taskDefinitionId: run.taskDefinitionId.toString(),
      taskDefinition: {
        ...run.taskDefinition,
        id: run.taskDefinition.id.toString(),
      },
      stepMeta: stepMetaMap.get(run.id.toString()) ?? null,
    }));
  }

  async getById(id: string) {
    const run = await this.prisma.taskRun.findUnique({
      where: { id: BigInt(id) },
      include: {
        taskDefinition: {
          select: {
            id: true,
            name: true,
            taskType: true,
          },
        },
      },
    });

    if (!run) throw new NotFoundException('task run not found');

    return {
      ...run,
      id: run.id.toString(),
      taskDefinitionId: run.taskDefinitionId.toString(),
      taskDefinition: {
        ...run.taskDefinition,
        id: run.taskDefinition.id.toString(),
      },
    };
  }

  async listSteps(taskRunId: string, params: { status?: TaskRunStepStatus; entityType?: TaskRunEntityType; limit?: number }) {
    const steps = await this.prisma.taskRunStep.findMany({
      where: {
        taskRunId: BigInt(taskRunId),
        status: params.status,
        entityType: params.entityType,
      },
      orderBy: { createdAt: 'desc' },
      take: params.limit ?? 200,
    });

    return steps.map((step) => ({
      ...step,
      id: step.id.toString(),
      taskRunId: step.taskRunId.toString(),
      entityId: step.entityId.toString(),
    }));
  }
}

import { TaskDefinitionType } from '@prisma/client';
import { MAX_SCHEDULE_BATCH, TASK_DEFINITION_ERROR_RETRY_SEC } from '../config/env';
import { getTaskDefinitionModel } from '../infra/prisma';
import { logger, logError } from '../logger';
import { safeRunInterval } from '../schedule-utils';
import { taskdefMetrics, METRICS_LOG_INTERVAL_TICKS } from '../shared/metrics';
import { releaseTaskDefinitionLock, tryAcquireTaskDefinitionLock } from '../shared/taskdef-lock';
import { updateTaskDefinitionRunStatus } from '../services/task-definition.service';
import { scheduleDispatchForDefinition } from './dispatch-scheduler';
import { scheduleRelayForDefinition } from './relay-scheduler';
import { scheduleCatalogForDefinition } from './catalog-scheduler';
import { scheduleDueMassMessageItems } from './mass-message-scheduler';

let hasWarnedMissingTaskDefinitionsTable = false;

export async function scheduleTaskDefinitionByType(definition: {
  id: bigint;
  taskType: TaskDefinitionType;
}) {
  if (definition.taskType === TaskDefinitionType.relay_upload) {
    await scheduleRelayForDefinition(definition.id);
    return;
  }

  if (definition.taskType === TaskDefinitionType.dispatch_send) {
    await scheduleDispatchForDefinition(definition.id);
    return;
  }

  if (definition.taskType === TaskDefinitionType.catalog_publish) {
    await scheduleCatalogForDefinition(definition.id);
    return;
  }

  if (definition.taskType === TaskDefinitionType.mass_message) {
    await scheduleDueMassMessageItems();

    await updateTaskDefinitionRunStatus({
      taskDefinitionId: definition.id,
      status: 'success',
      summary: {
        executor: 'mass_message',
        message: '群发调度完成',
      },
    });
  }
}

export async function scheduleEnabledTaskDefinitions() {
  taskdefMetrics.tickTotal += 1;

  let definitions: Array<{
    id: bigint;
    taskType: TaskDefinitionType;
    runIntervalSec: number;
    nextRunAt: Date | null;
  }> = [];

  try {
    definitions = await getTaskDefinitionModel().findMany({
      where: {
        isEnabled: true,
        OR: [{ nextRunAt: null }, { nextRunAt: { lte: new Date() } }],
      },
      orderBy: [{ priority: 'asc' }, { nextRunAt: 'asc' }, { updatedAt: 'asc' }],
      take: MAX_SCHEDULE_BATCH,
      select: {
        id: true,
        taskType: true,
        runIntervalSec: true,
        nextRunAt: true,
      },
    });
  } catch (error) {
    const prismaCode =
      typeof error === 'object' && error !== null && 'code' in error
        ? (error as { code?: string }).code
        : undefined;

    const isSchemaNotReady = prismaCode === 'P2021' || prismaCode === 'P2022';

    if (isSchemaNotReady) {
      if (!hasWarnedMissingTaskDefinitionsTable) {
        logger.warn('[scheduler:task-definitions] task_definitions 表未就绪，已回退到旧调度器。请运行 prisma migrate 启用任务定义调度。');
        hasWarnedMissingTaskDefinitionsTable = true;
      }

      await scheduleDispatchForDefinition(BigInt(0));
      await scheduleRelayForDefinition(BigInt(0));
      await scheduleCatalogForDefinition(BigInt(0));
      await scheduleDueMassMessageItems();
      return;
    }

    throw error;
  }

  taskdefMetrics.dueTotal += definitions.length;

  for (const definition of definitions) {
    const lockToken = await tryAcquireTaskDefinitionLock(definition.id);
    if (!lockToken) {
      taskdefMetrics.lockSkipTotal += 1;
      logger.info('[scheduler:taskdef] 跳过执行（锁未获取）', {
        taskDefinitionId: definition.id.toString(),
        taskType: definition.taskType,
      });
      continue;
    }

    const runStart = Date.now();
    const now = new Date();
    const safeRunIntervalSec = safeRunInterval(definition.runIntervalSec);
    const nextRunAtBefore = definition.nextRunAt?.toISOString() ?? null;
    let runStatus: 'success' | 'failed' = 'success';
    let nextRunAtAfter: string | null = null;

    try {
      await getTaskDefinitionModel().update({
        where: { id: definition.id },
        data: {
          lastStartedAt: now,
        },
      });

      await scheduleTaskDefinitionByType({
        ...definition,
      });

      const newNextRunAt = new Date(now.getTime() + safeRunIntervalSec * 1000);
      nextRunAtAfter = newNextRunAt.toISOString();

      await getTaskDefinitionModel().update({
        where: { id: definition.id },
        data: {
          nextRunAt: newNextRunAt,
        },
      });

      taskdefMetrics.runSuccessTotal += 1;
    } catch (error) {
      runStatus = 'failed';
      taskdefMetrics.runFailedTotal += 1;

      const errorNextRunAt = new Date(
        now.getTime() +
          Math.min(safeRunIntervalSec, TASK_DEFINITION_ERROR_RETRY_SEC) * 1000,
      );
      nextRunAtAfter = errorNextRunAt.toISOString();

      await getTaskDefinitionModel().update({
        where: { id: definition.id },
        data: {
          nextRunAt: errorNextRunAt,
        },
      });

      logError('[scheduler:taskdef] 执行失败', {
        taskDefinitionId: definition.id.toString(),
        taskType: definition.taskType,
        error: error instanceof Error ? error.message : '未知错误',
      });
    } finally {
      const durationMs = Date.now() - runStart;
      taskdefMetrics.runDurationMsTotal += durationMs;

      logger.info('taskdef_run', {
        tag: 'taskdef_run',
        message: '任务定义调度运行记录',
        taskDefinitionId: definition.id.toString(),
        taskType: definition.taskType,
        runIntervalSec: safeRunIntervalSec,
        lockAcquired: true,
        status: runStatus,
        durationMs,
        nextRunAtBefore,
        nextRunAtAfter,
      });

      await releaseTaskDefinitionLock(definition.id, lockToken);
    }
  }

  if (taskdefMetrics.tickTotal % METRICS_LOG_INTERVAL_TICKS === 0) {
    logger.info('taskdef_metrics', {
      tag: 'taskdef_metrics',
      message: '任务定义调度指标',
      ...taskdefMetrics,
      avgRunDurationMs:
        taskdefMetrics.runSuccessTotal + taskdefMetrics.runFailedTotal > 0
          ? Math.round(
              taskdefMetrics.runDurationMsTotal /
                (taskdefMetrics.runSuccessTotal + taskdefMetrics.runFailedTotal),
            )
          : 0,
    });
  }
}

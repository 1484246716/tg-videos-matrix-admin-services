import {
  CATALOG_CHANNEL_INTERVAL_GUARD_ENABLED,
  TYPEC_ALERT_EMPTY_RUN_CONSECUTIVE_THRESHOLD,
  TYPEC_ALERT_FAILED_RUN_THRESHOLD,
  TYPEC_ALERT_MANUAL_REPAIR_QUEUE_LAG_SECONDS,
  TYPEC_METRICS_LOG_INTERVAL_TICKS,
  TYPEC_SELF_HEAL_ENABLED,
  TYPEC_SELF_HEAL_ON_SKIP,
} from '../config/env';
import { prisma } from '../infra/prisma';
import { catalogQueue } from '../infra/redis';
import { logger } from '../logger';
import { releaseChannelLock, tryAcquireChannelLock } from '../shared/channel-lock';
import { catalogMetrics } from '../shared/metrics';
import { updateTaskDefinitionRunStatus } from '../services/task-definition.service';

function computeCatalogNextAllowedAt(args: {
  lastNavUpdateAt: Date | null;
  navIntervalSec: number;
  now: Date;
}) {
  if (!args.lastNavUpdateAt) return args.now;
  return new Date(args.lastNavUpdateAt.getTime() + Math.max(0, args.navIntervalSec) * 1000);
}

export async function scheduleDueCatalogTasks() {
  catalogMetrics.tickTotal += 1;
  const now = new Date();

  const channels = await prisma.channel.findMany({
    where: {
      status: 'active',
      navEnabled: true,
    },
    select: {
      id: true,
      name: true,
      lastNavUpdateAt: true,
      navIntervalSec: true,
      navTemplateText: true,
      defaultBotId: true,
    },
  });

  let queuedCount = 0;

  for (const channel of channels) {
    if (!channel.navTemplateText || !channel.defaultBotId) continue;

    if (CATALOG_CHANNEL_INTERVAL_GUARD_ENABLED) {
      const nextAllowedAt = computeCatalogNextAllowedAt({
        lastNavUpdateAt: channel.lastNavUpdateAt,
        navIntervalSec: channel.navIntervalSec,
        now,
      });
      if (nextAllowedAt.getTime() > now.getTime()) {
        logger.info('[scheduler] 目录任务未到频道更新窗口', {
          channelId: channel.id.toString(),
          navIntervalSec: channel.navIntervalSec,
          lastNavUpdateAt: channel.lastNavUpdateAt?.toISOString() ?? null,
          nextAllowedAt: nextAllowedAt.toISOString(),
          selfHealOnSkip: TYPEC_SELF_HEAL_ENABLED && TYPEC_SELF_HEAL_ON_SKIP,
        });

        if (!(TYPEC_SELF_HEAL_ENABLED && TYPEC_SELF_HEAL_ON_SKIP)) {
          continue;
        }
      }
    }

    const lock = await tryAcquireChannelLock({ scope: 'catalog', channelId: channel.id });
    if (!lock.acquired) {
      logger.info('[scheduler] 目录任务跳过（频道锁未获取）', {
        channelId: channel.id.toString(),
        lockKey: lock.lockKey,
      });
      continue;
    }

    try {
    const jobId = `catalog-${channel.id.toString()}`;
    const existingJob = await catalogQueue.getJob(jobId);
    if (existingJob) {
      const state = await existingJob.getState();
      if (state === 'failed') {
        await existingJob.remove();
      } else {
        continue;
      }
    }

    await catalogQueue.add(
      'catalog-publish',
      {
        channelIdRaw: channel.id.toString(),
        selfHealOnly:
          Boolean(TYPEC_SELF_HEAL_ENABLED && TYPEC_SELF_HEAL_ON_SKIP) &&
          CATALOG_CHANNEL_INTERVAL_GUARD_ENABLED &&
          channel.lastNavUpdateAt
            ? computeCatalogNextAllowedAt({
                lastNavUpdateAt: channel.lastNavUpdateAt,
                navIntervalSec: channel.navIntervalSec,
                now,
              }).getTime() > now.getTime()
            : false,
      },
      {
        jobId,
        removeOnComplete: true,
        removeOnFail: 200,
      },
    );

    queuedCount += 1;
    } finally {
      await releaseChannelLock({ lockKey: lock.lockKey, lockToken: lock.lockToken });
    }
  }

  if (queuedCount > 0) {
    logger.info('[scheduler] 已入队频道导航任务', { count: queuedCount });
  }

  if (catalogMetrics.tickTotal % TYPEC_METRICS_LOG_INTERVAL_TICKS === 0) {
    const publishRuns = catalogMetrics.publishRunSuccessTotal + catalogMetrics.publishRunFailedTotal;
    const avgDurationMs =
      publishRuns > 0 ? Math.round(catalogMetrics.publishDurationMsTotal / publishRuns) : 0;

    logger.info('typec_metrics', {
      tag: 'typec_metrics',
      message: 'TypeC 目录链路指标快照',
      ...catalogMetrics,
      avgDurationMs,
      alertThresholds: {
        emptyRunConsecutive: TYPEC_ALERT_EMPTY_RUN_CONSECUTIVE_THRESHOLD,
        failedRun: TYPEC_ALERT_FAILED_RUN_THRESHOLD,
        manualRepairQueueLagSeconds: TYPEC_ALERT_MANUAL_REPAIR_QUEUE_LAG_SECONDS,
      },
    });
  }

  if (catalogMetrics.publishEmptyRunConsecutive >= TYPEC_ALERT_EMPTY_RUN_CONSECUTIVE_THRESHOLD) {
    logger.error('typec_alert_empty_run_consecutive', {
      tag: 'typec_alert_empty_run_consecutive',
      message: 'TypeC 连续空目录发布告警',
      consecutive: catalogMetrics.publishEmptyRunConsecutive,
      threshold: TYPEC_ALERT_EMPTY_RUN_CONSECUTIVE_THRESHOLD,
      suggestion: '请检查 catalog_source_item 是否有数据写入，以及读源开关是否正确。',
    });
  }

  if (catalogMetrics.publishRunFailedTotal >= TYPEC_ALERT_FAILED_RUN_THRESHOLD) {
    logger.error('typec_alert_failed_run_spike', {
      tag: 'typec_alert_failed_run_spike',
      message: 'TypeC 发布失败次数达到告警阈值',
      failedTotal: catalogMetrics.publishRunFailedTotal,
      threshold: TYPEC_ALERT_FAILED_RUN_THRESHOLD,
      suggestion: '请检查 worker 错误日志与 Telegram API 可用性。',
    });
  }
}

export async function scheduleCatalogForDefinition(taskDefinitionId: bigint) {
  try {
    await scheduleDueCatalogTasks();
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'success',
      summary: {
        executor: 'catalog_publish',
        message: '频道导航调度完成',
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : '未知错误';
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'failed',
      summary: {
        executor: 'catalog_publish',
        error: `频道导航调度失败: ${message}`,
      },
    });

    throw error;
  }
}

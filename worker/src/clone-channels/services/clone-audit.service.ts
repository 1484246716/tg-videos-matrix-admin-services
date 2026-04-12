import { prisma } from '../../infra/prisma';
import { cloneGuardWaitQueue, cloneMediaDownloadQueue, cloneRetryQueue } from '../../infra/redis';
import { logger } from '../../logger';
import { CLONE_DOWNLOAD_STUCK_MS } from '../../config/env';

const CLONE_ALERT_RETRY_EXHAUSTED_SPIKE_THRESHOLD = (() => {
  const n = Number(process.env.CLONE_ALERT_RETRY_EXHAUSTED_SPIKE_THRESHOLD ?? '5');
  if (!Number.isFinite(n) || n < 1) return 5;
  return Math.min(200, Math.floor(n));
})();

const CLONE_ALERT_GUARD_WAIT_STUCK_MINUTES = (() => {
  const n = Number(process.env.CLONE_ALERT_GUARD_WAIT_STUCK_MINUTES ?? '10');
  if (!Number.isFinite(n) || n < 1) return 10;
  return Math.min(240, Math.floor(n));
})();

const CLONE_ALERT_GROUP_BLOCKED_THRESHOLD = (() => {
  const n = Number(process.env.CLONE_ALERT_GROUP_BLOCKED_THRESHOLD ?? '3');
  if (!Number.isFinite(n) || n < 1) return 3;
  return Math.min(200, Math.floor(n));
})();

const CLONE_ALERT_GROUP_BLOCKED_SPIKE_THRESHOLD = (() => {
  const n = Number(process.env.CLONE_ALERT_GROUP_BLOCKED_SPIKE_THRESHOLD ?? '10');
  if (!Number.isFinite(n) || n < 1) return 10;
  return Math.min(1000, Math.floor(n));
})();

let guardWaitStuckSinceAt: number | null = null;
let lastAlertState = {
  retryExhaustedSpike: false,
  guardWaitStuck: false,
  groupBlocked: false,
};

export async function auditCloneHealth() {
  const recentWindow = new Date(Date.now() - 10 * 60 * 1000);
  const staleBefore = new Date(Date.now() - CLONE_DOWNLOAD_STUCK_MS);

  const [
    retryExhaustedRecent,
    failedFinalRecent,
    guardWaitingCount,
    downloadWaitingCount,
    downloadActiveCount,
    retryWaitingCount,
    groupBlockedRows,
  ] = await Promise.all([
    prisma.cloneCrawlItem.count({
      where: {
        downloadStatus: 'failed_final',
        downloadErrorCode: 'retry_exhausted',
        updatedAt: { gte: recentWindow },
      },
    }),
    prisma.cloneCrawlItem.count({
      where: {
        downloadStatus: 'failed_final',
        updatedAt: { gte: recentWindow },
      },
    }),
    cloneGuardWaitQueue.getWaitingCount(),
    cloneMediaDownloadQueue.getWaitingCount(),
    cloneMediaDownloadQueue.getActiveCount(),
    cloneRetryQueue.getWaitingCount(),
    prisma.cloneCrawlItem.groupBy({
      by: ['groupKey'],
      where: {
        groupKey: { not: null },
        downloadStatus: { in: ['queued', 'downloading', 'failed_retryable'] },
        updatedAt: { lte: staleBefore },
      },
      _count: { _all: true },
      orderBy: {
        _count: {
          groupKey: 'desc',
        },
      },
      take: 20,
    } as any),
  ]);

  const groupBlockedCount = (groupBlockedRows || []).filter((row: any) => Number(row?._count?._all ?? 0) >= CLONE_ALERT_GROUP_BLOCKED_THRESHOLD).length;

  const nowMs = Date.now();
  if (guardWaitingCount > 0 && downloadActiveCount === 0) {
    if (!guardWaitStuckSinceAt) {
      guardWaitStuckSinceAt = nowMs;
    }
  } else {
    guardWaitStuckSinceAt = null;
  }

  const guardWaitStuckMinutes = guardWaitStuckSinceAt
    ? Math.floor((nowMs - guardWaitStuckSinceAt) / 60000)
    : 0;

  const retryExhaustedSpike = retryExhaustedRecent >= CLONE_ALERT_RETRY_EXHAUSTED_SPIKE_THRESHOLD;
  const guardWaitStuck = guardWaitStuckMinutes >= CLONE_ALERT_GUARD_WAIT_STUCK_MINUTES;
  const groupBlocked = groupBlockedCount > 0;
  const groupBlockedSpike = groupBlockedCount >= CLONE_ALERT_GROUP_BLOCKED_SPIKE_THRESHOLD;

  logger.info('[clone_audit] health snapshot', {
    clone_retry_exhausted_recent_total: retryExhaustedRecent,
    clone_failed_final_recent_total: failedFinalRecent,
    clone_guard_wait_waiting: guardWaitingCount,
    clone_download_waiting: downloadWaitingCount,
    clone_download_active: downloadActiveCount,
    clone_retry_waiting: retryWaitingCount,
    clone_guard_wait_stuck_minutes: guardWaitStuckMinutes,
    clone_download_group_blocked_total: groupBlockedCount,
    clone_download_group_blocked_top: (groupBlockedRows || []).map((row: any) => ({
      groupKey: row.groupKey,
      stuckCount: Number(row?._count?._all ?? 0),
    })),
    alert_retry_exhausted_spike_threshold: CLONE_ALERT_RETRY_EXHAUSTED_SPIKE_THRESHOLD,
    alert_guard_wait_stuck_minutes_threshold: CLONE_ALERT_GUARD_WAIT_STUCK_MINUTES,
    alert_group_blocked_threshold: CLONE_ALERT_GROUP_BLOCKED_THRESHOLD,
    alert_group_blocked_spike_threshold: CLONE_ALERT_GROUP_BLOCKED_SPIKE_THRESHOLD,
    alert_retry_exhausted_spike_triggered: retryExhaustedSpike,
    alert_guard_wait_stuck_triggered: guardWaitStuck,
    alert_group_blocked_triggered: groupBlocked,
    alert_group_blocked_spike_triggered: groupBlockedSpike,
    metric_labels: {
      clone_retry_exhausted_recent_total: 'Clone 近10分钟 retry_exhausted 失败终态数',
      clone_failed_final_recent_total: 'Clone 近10分钟 failed_final 总数',
      clone_guard_wait_waiting: 'Clone guard_wait 队列等待数',
      clone_download_waiting: 'Clone download 队列等待数',
      clone_download_active: 'Clone download 队列执行中数',
      clone_retry_waiting: 'Clone retry 队列等待数',
      clone_download_group_blocked_total: 'Clone 组级卡住 groupKey 数',
    },
  });

  const alertChanged =
    retryExhaustedSpike !== lastAlertState.retryExhaustedSpike ||
    guardWaitStuck !== lastAlertState.guardWaitStuck ||
    groupBlocked !== lastAlertState.groupBlocked;

  if (retryExhaustedSpike || guardWaitStuck || groupBlocked || groupBlockedSpike) {
    if (alertChanged) {
      logger.warn('[clone_alert] threshold triggered', {
        retryExhaustedRecent,
        failedFinalRecent,
        guardWaitingCount,
        downloadWaitingCount,
        downloadActiveCount,
        retryWaitingCount,
        guardWaitStuckMinutes,
        groupBlockedCount,
        topBlockedGroups: (groupBlockedRows || []).map((row: any) => ({
          groupKey: row.groupKey,
          stuckCount: Number(row?._count?._all ?? 0),
        })),
        alert_retry_exhausted_spike_triggered: retryExhaustedSpike,
        alert_guard_wait_stuck_triggered: guardWaitStuck,
        alert_group_blocked_triggered: groupBlocked,
      });
    }
  } else if (alertChanged) {
    logger.info('[clone_alert] recovered', {
      retryExhaustedRecent,
      failedFinalRecent,
      guardWaitingCount,
      downloadWaitingCount,
      downloadActiveCount,
      retryWaitingCount,
      guardWaitStuckMinutes,
      groupBlockedCount,
      alert_retry_exhausted_spike_triggered: retryExhaustedSpike,
      alert_guard_wait_stuck_triggered: guardWaitStuck,
      alert_group_blocked_triggered: groupBlocked,
    });
  }

  lastAlertState = {
    retryExhaustedSpike,
    guardWaitStuck,
    groupBlocked,
  };
}

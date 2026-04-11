import { prisma } from '../../infra/prisma';
import { cloneGuardWaitQueue, cloneVideoDownloadQueue, cloneRetryQueue } from '../../infra/redis';
import { logger } from '../../logger';

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

let guardWaitStuckSinceAt: number | null = null;
let lastAlertState = {
  retryExhaustedSpike: false,
  guardWaitStuck: false,
};

export async function auditCloneHealth() {
  const recentWindow = new Date(Date.now() - 10 * 60 * 1000);

  const [
    retryExhaustedRecent,
    failedFinalRecent,
    guardWaitingCount,
    downloadWaitingCount,
    downloadActiveCount,
    retryWaitingCount,
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
    cloneVideoDownloadQueue.getWaitingCount(),
    cloneVideoDownloadQueue.getActiveCount(),
    cloneRetryQueue.getWaitingCount(),
  ]);

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

  logger.info('[clone_audit] health snapshot', {
    clone_retry_exhausted_recent_total: retryExhaustedRecent,
    clone_failed_final_recent_total: failedFinalRecent,
    clone_guard_wait_waiting: guardWaitingCount,
    clone_download_waiting: downloadWaitingCount,
    clone_download_active: downloadActiveCount,
    clone_retry_waiting: retryWaitingCount,
    clone_guard_wait_stuck_minutes: guardWaitStuckMinutes,
    alert_retry_exhausted_spike_threshold: CLONE_ALERT_RETRY_EXHAUSTED_SPIKE_THRESHOLD,
    alert_guard_wait_stuck_minutes_threshold: CLONE_ALERT_GUARD_WAIT_STUCK_MINUTES,
    alert_retry_exhausted_spike_triggered: retryExhaustedSpike,
    alert_guard_wait_stuck_triggered: guardWaitStuck,
    metric_labels: {
      clone_retry_exhausted_recent_total: 'Clone 近10分钟 retry_exhausted 失败终态数',
      clone_failed_final_recent_total: 'Clone 近10分钟 failed_final 总数',
      clone_guard_wait_waiting: 'Clone guard_wait 队列等待数',
      clone_download_waiting: 'Clone download 队列等待数',
      clone_download_active: 'Clone download 队列执行中数',
      clone_retry_waiting: 'Clone retry 队列等待数',
    },
  });

  const alertChanged =
    retryExhaustedSpike !== lastAlertState.retryExhaustedSpike ||
    guardWaitStuck !== lastAlertState.guardWaitStuck;

  if (retryExhaustedSpike || guardWaitStuck) {
    if (alertChanged) {
      logger.warn('[clone_alert] threshold triggered', {
        retryExhaustedRecent,
        failedFinalRecent,
        guardWaitingCount,
        downloadWaitingCount,
        downloadActiveCount,
        retryWaitingCount,
        guardWaitStuckMinutes,
        alert_retry_exhausted_spike_triggered: retryExhaustedSpike,
        alert_guard_wait_stuck_triggered: guardWaitStuck,
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
      alert_retry_exhausted_spike_triggered: retryExhaustedSpike,
      alert_guard_wait_stuck_triggered: guardWaitStuck,
    });
  }

  lastAlertState = {
    retryExhaustedSpike,
    guardWaitStuck,
  };
}

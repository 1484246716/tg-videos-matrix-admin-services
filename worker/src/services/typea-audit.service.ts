import { MediaStatus } from '@prisma/client';
import {
  TYPEA_ALERT_FAILED_FINAL_SPIKE_THRESHOLD,
  TYPEA_ALERT_QUEUE_STUCK_MINUTES,
  TYPEA_ALERT_STALE_THRESHOLD,
} from '../config/env';
import { prisma } from '../infra/prisma';
import { relayUploadQueue } from '../infra/redis';
import { logger } from '../logger';

const AUDIT_SAMPLE_LIMIT = 20;

let queueStuckSinceAt: number | null = null;

export async function auditTypeAHealth() {
  const stale10Min = new Date(Date.now() - 10 * 60 * 1000);

  const [
    ingestingStaleAssets,
    failedFinalAssets,
    failedMissingAssets,
    waitingCount,
    activeCount,
    failedCount,
    delayedCount,
  ] = await Promise.all([
    prisma.mediaAsset.findMany({
      where: {
        status: MediaStatus.ingesting,
        updatedAt: { lte: stale10Min },
        telegramFileId: null,
      },
      select: { id: true, channelId: true },
      take: AUDIT_SAMPLE_LIMIT,
      orderBy: { updatedAt: 'asc' },
    }),
    prisma.mediaAsset.findMany({
      where: {
        status: MediaStatus.failed,
        sourceMeta: {
          path: ['ingestFinalReason'],
          equals: 'FAILED_FINAL',
        },
      },
      select: { id: true, channelId: true },
      take: AUDIT_SAMPLE_LIMIT,
    }),
    prisma.mediaAsset.findMany({
      where: {
        status: MediaStatus.failed,
        sourceMeta: {
          path: ['ingestErrorCode'],
          equals: 'SRC_FILE_MISSING',
        },
      },
      select: { id: true, channelId: true },
      take: AUDIT_SAMPLE_LIMIT,
    }),
    relayUploadQueue.getWaitingCount(),
    relayUploadQueue.getActiveCount(),
    relayUploadQueue.getFailedCount(),
    relayUploadQueue.getDelayedCount(),
  ]);

  const byChannel = new Map<string, { stale: number; failedFinal: number; missing: number }>();

  const nowMs = Date.now();
  if (activeCount === 0 && waitingCount > 0) {
    if (!queueStuckSinceAt) {
      queueStuckSinceAt = nowMs;
    }
  } else {
    queueStuckSinceAt = null;
  }

  const queueStuckMinutes = queueStuckSinceAt
    ? Math.floor((nowMs - queueStuckSinceAt) / 60000)
    : 0;

  const staleAlert = ingestingStaleAssets.length > TYPEA_ALERT_STALE_THRESHOLD;
  const failedFinalSpikeAlert =
    failedFinalAssets.length >= TYPEA_ALERT_FAILED_FINAL_SPIKE_THRESHOLD;
  const queueStuckAlert =
    queueStuckMinutes >= TYPEA_ALERT_QUEUE_STUCK_MINUTES && waitingCount > 0 && activeCount === 0;

  const mark = (channelId: bigint, key: 'stale' | 'failedFinal' | 'missing') => {
    const id = channelId.toString();
    const current = byChannel.get(id) ?? { stale: 0, failedFinal: 0, missing: 0 };
    current[key] += 1;
    byChannel.set(id, current);
  };

  ingestingStaleAssets.forEach((row) => mark(row.channelId, 'stale'));
  failedFinalAssets.forEach((row) => mark(row.channelId, 'failedFinal'));
  failedMissingAssets.forEach((row) => mark(row.channelId, 'missing'));

  const topChannels = [...byChannel.entries()]
    .sort((a, b) => {
      const ta = a[1].stale + a[1].failedFinal + a[1].missing;
      const tb = b[1].stale + b[1].failedFinal + b[1].missing;
      return tb - ta;
    })
    .slice(0, 10)
    .map(([channelId, stats]) => ({ channelId, ...stats }));

  logger.info('[typea_audit] health snapshot', {
    task_stale_total: ingestingStaleAssets.length,
    task_dead_total: failedFinalAssets.length,
    typea_file_missing_total: failedMissingAssets.length,
    typea_queue_waiting: waitingCount,
    typea_queue_active: activeCount,
    typea_queue_failed: failedCount,
    typea_queue_delayed: delayedCount,
    topChannels,
    alert_stale_threshold: TYPEA_ALERT_STALE_THRESHOLD,
    alert_failed_final_spike_threshold: TYPEA_ALERT_FAILED_FINAL_SPIKE_THRESHOLD,
    alert_queue_stuck_minutes_threshold: TYPEA_ALERT_QUEUE_STUCK_MINUTES,
    alert_stale_triggered: staleAlert,
    alert_failed_final_spike_triggered: failedFinalSpikeAlert,
    alert_queue_stuck_triggered: queueStuckAlert,
    queue_stuck_minutes: queueStuckMinutes,
    metric_labels: {
      task_stale_total: 'TypeA stale 任务总数（审计样本）',
      task_dead_total: 'TypeA 失败终态总数（审计样本）',
      typea_file_missing_total: 'TypeA 源文件缺失总数（审计样本）',
      typea_queue_waiting: 'TypeA 队列等待中任务数',
      typea_queue_active: 'TypeA 队列执行中任务数',
      typea_queue_failed: 'TypeA 队列失败任务数',
    },
  });

  if (staleAlert || failedFinalSpikeAlert || queueStuckAlert) {
    logger.warn('[typea_alert] threshold triggered', {
      task_stale_total: ingestingStaleAssets.length,
      task_dead_total: failedFinalAssets.length,
      typea_queue_waiting: waitingCount,
      typea_queue_active: activeCount,
      queue_stuck_minutes: queueStuckMinutes,
      alert_stale_triggered: staleAlert,
      alert_failed_final_spike_triggered: failedFinalSpikeAlert,
      alert_queue_stuck_triggered: queueStuckAlert,
      topChannels,
    });
  }
}

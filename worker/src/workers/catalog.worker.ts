/**
 * Catalog Worker：消费目录队列并执行目录任务处理。
 * 在 bootstrap 注册后由 BullMQ 驱动，负责接收 job 并调用 catalog service。
 */

import { Worker } from 'bullmq';
import { TYPEC_ALERT_MANUAL_REPAIR_QUEUE_LAG_SECONDS } from '../config/env';
import { connection } from '../infra/redis';
import { logger, logError } from '../logger';
import { handleCatalogJob } from '../services/catalog.service';

export const catalogWorker = new Worker(
  'q_catalog',
  async (job) => {
    if (job.name === 'bootstrap-check') {
      return { ok: true, skipped: true, reason: 'bootstrap-check' };
    }

    const channelIdRaw = job.data.channelIdRaw as string | undefined;
    if (!channelIdRaw) {
      throw new Error('任务负载缺少 channelIdRaw');
    }

    const payload = (job.data as {
      selfHealOnly?: boolean;
      manualRepair?: boolean;
      runId?: string;
    } | undefined) || {};

    const selfHealOnly = Boolean(payload.selfHealOnly);
    const triggerType = payload.manualRepair ? 'manual_repair' : 'scheduler';

    if (triggerType === 'manual_repair' && typeof job.timestamp === 'number') {
      const queueLagSeconds = Math.max(0, Math.floor((Date.now() - job.timestamp) / 1000));
      if (queueLagSeconds >= TYPEC_ALERT_MANUAL_REPAIR_QUEUE_LAG_SECONDS) {
        logger.error('typec_alert_manual_repair_queue_lag', {
          tag: 'typec_alert_manual_repair_queue_lag',
          message: 'TypeC 手动修复队列等待超阈值',
          jobId: String(job.id),
          channelIdRaw,
          queueLagSeconds,
          threshold: TYPEC_ALERT_MANUAL_REPAIR_QUEUE_LAG_SECONDS,
        });
      }
    }

    return handleCatalogJob(channelIdRaw, {
      selfHealOnly,
      triggerType,
      runId: payload.runId,
    });
  },
  { connection: connection as any, concurrency: 3 },
);

catalogWorker.on('completed', (job) => {
  logger.info('[q_catalog] 任务完成', { jobId: String(job.id) });
});

catalogWorker.on('failed', (job, err) => {
  logger.error('[q_catalog] 任务失败', {
    jobId: job?.id ? String(job.id) : null,
    jobName: job?.name ?? null,
    channelIdRaw: (job?.data as any)?.channelIdRaw ?? null,
    errName: err?.name ?? null,
    errMessage: err?.message ?? null,
    errStack: err?.stack ?? null,
  });

  logError('[q_catalog] 任务失败(兼容日志)', err);
});

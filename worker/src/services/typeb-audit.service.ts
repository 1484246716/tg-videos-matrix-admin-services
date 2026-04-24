/**
 * ?????TypeB ????????? grouped dispatch ????????????????
 * ?????bootstrap ???? -> auditTypeBGroupSendHealth -> ????????
 */

import { TaskStatus } from '@prisma/client';
import {
  TYPEB_GROUP_SEND_ALERT_DEAD_SPIKE_THRESHOLD,
  TYPEB_GROUP_SEND_ALERT_FAILED_SPIKE_THRESHOLD,
  TYPEB_GROUP_SEND_ENABLED,
} from '../config/env';
import { prisma } from '../infra/prisma';
import { logger } from '../logger';

let lastTypeBAlertState = {
  failedSpike: false,
  deadSpike: false,
};

// ?? TypeB ????????????????????
export async function auditTypeBGroupSendHealth() {
  if (!TYPEB_GROUP_SEND_ENABLED) return;

  const recentWindow = new Date(Date.now() - 5 * 60 * 1000);

  const [failedSpikeCount, deadSpikeCount, successRecent] = await Promise.all([
    prisma.dispatchTaskLog.count({
      where: {
        action: 'task_failed_group_send',
        createdAt: { gte: recentWindow },
      },
    }),
    prisma.dispatchTaskLog.count({
      where: {
        action: 'task_dead_group_send',
        createdAt: { gte: recentWindow },
      },
    }),
    prisma.dispatchTaskLog.count({
      where: {
        action: 'task_success',
        createdAt: { gte: recentWindow },
        dispatchTask: {
          groupKey: { not: null },
          status: TaskStatus.success,
        },
      },
    }),
  ]);

  const failedSpike = failedSpikeCount >= TYPEB_GROUP_SEND_ALERT_FAILED_SPIKE_THRESHOLD;
  const deadSpike = deadSpikeCount >= TYPEB_GROUP_SEND_ALERT_DEAD_SPIKE_THRESHOLD;

  logger.info('[typeb_metrics] group send health snapshot', {
    typeb_group_send_failed_5m_total: failedSpikeCount,
    typeb_group_send_dead_5m_total: deadSpikeCount,
    typeb_group_send_success_5m_total: successRecent,
    alert_typeb_group_send_failed_spike_threshold: TYPEB_GROUP_SEND_ALERT_FAILED_SPIKE_THRESHOLD,
    alert_typeb_group_send_dead_spike_threshold: TYPEB_GROUP_SEND_ALERT_DEAD_SPIKE_THRESHOLD,
    alert_typeb_group_send_failed_spike_triggered: failedSpike,
    alert_typeb_group_send_dead_spike_triggered: deadSpike,
    metric_labels: {
      typeb_group_send_failed_5m_total: 'TypeB 近5分钟 group send 失败数',
      typeb_group_send_dead_5m_total: 'TypeB 近5分钟 group send dead 数',
      typeb_group_send_success_5m_total: 'TypeB 近5分钟 group send 成功数',
    },
  });

  const alertChanged =
    failedSpike !== lastTypeBAlertState.failedSpike || deadSpike !== lastTypeBAlertState.deadSpike;

  if (failedSpike || deadSpike) {
    if (alertChanged) {
      logger.warn('[typeb_alert] group send spike triggered', {
        typeb_group_send_failed_5m_total: failedSpikeCount,
        typeb_group_send_dead_5m_total: deadSpikeCount,
        threshold_failed: TYPEB_GROUP_SEND_ALERT_FAILED_SPIKE_THRESHOLD,
        threshold_dead: TYPEB_GROUP_SEND_ALERT_DEAD_SPIKE_THRESHOLD,
        alert_typeb_group_send_failed_spike_triggered: failedSpike,
        alert_typeb_group_send_dead_spike_triggered: deadSpike,
      });
    }
  } else if (alertChanged) {
    logger.info('[typeb_alert] group send recovered', {
      typeb_group_send_failed_5m_total: failedSpikeCount,
      typeb_group_send_dead_5m_total: deadSpikeCount,
      alert_typeb_group_send_failed_spike_triggered: failedSpike,
      alert_typeb_group_send_dead_spike_triggered: deadSpike,
    });
  }

  lastTypeBAlertState = {
    failedSpike,
    deadSpike,
  };
}

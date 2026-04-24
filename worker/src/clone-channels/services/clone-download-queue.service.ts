/**
 * Clone Channels 下载队列服务：负责下载任务 jobId 规范、在途判定与安全入队准备。
 * 用于在 clone 调度/执行链路中避免重复下载任务并处理终态任务清理。
 */

import { cloneMediaDownloadQueue } from '../../infra/redis';
import { logger } from '../../logger';

const NON_TERMINAL_STATES = new Set([
  'active',
  'waiting',
  'delayed',
  'prioritized',
  'waiting-children',
]);

// 构造下载队列的稳定 jobId，确保同一 item 使用同一个任务标识。
export function buildCloneDownloadJobId(itemId: string | number | bigint) {
  return `clone-download-item-${String(itemId)}`;
}

// 判断下载任务是否仍在途（非终态），用于防止重复调度。
export async function hasCloneDownloadJobInFlight(itemId: string | number | bigint) {
  const job = await cloneMediaDownloadQueue.getJob(buildCloneDownloadJobId(itemId));
  if (!job) return false;

  const state = await job.getState();
  return NON_TERMINAL_STATES.has(state);
}

// 入队前准备：若存在终态旧任务则清理，若在途则阻止重复入队。
export async function prepareCloneDownloadJobForEnqueue(params: {
  itemId: string | number | bigint;
  source: string;
}) {
  const jobId = buildCloneDownloadJobId(params.itemId);
  const existing = await cloneMediaDownloadQueue.getJob(jobId);

  if (!existing) {
    return {
      jobId,
      canEnqueue: true as const,
      previousState: null as string | null,
    };
  }

  const state = await existing.getState();
  if (NON_TERMINAL_STATES.has(state)) {
    return {
      jobId,
      canEnqueue: false as const,
      reason: 'existing_non_terminal' as const,
      existingState: state,
    };
  }

  try {
    await existing.remove();
    logger.info('[clone][download-queue] removed terminal job before enqueue', {
      itemId: String(params.itemId),
      jobId,
      previousState: state,
      source: params.source,
    });

    return {
      jobId,
      canEnqueue: true as const,
      previousState: state,
    };
  } catch (err) {
    logger.warn('[clone][download-queue] failed to remove terminal job before enqueue', {
      itemId: String(params.itemId),
      jobId,
      previousState: state,
      source: params.source,
      error: err instanceof Error ? err.message : String(err),
    });

    return {
      jobId,
      canEnqueue: false as const,
      reason: 'terminal_remove_failed' as const,
      existingState: state,
    };
  }
}

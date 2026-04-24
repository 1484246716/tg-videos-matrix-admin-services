import { cloneMediaDownloadQueue } from '../../infra/redis';
import { logger } from '../../logger';

const NON_TERMINAL_STATES = new Set([
  'active',
  'waiting',
  'delayed',
  'prioritized',
  'waiting-children',
]);

export function buildCloneDownloadJobId(itemId: string | number | bigint) {
  return `clone-download-item-${String(itemId)}`;
}

export async function hasCloneDownloadJobInFlight(itemId: string | number | bigint) {
  const job = await cloneMediaDownloadQueue.getJob(buildCloneDownloadJobId(itemId));
  if (!job) return false;

  const state = await job.getState();
  return NON_TERMINAL_STATES.has(state);
}

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

/**
 * Clone Channels 下载队列服务测试：验证入队前去重与状态判定逻辑。
 * 覆盖 clone 调度/执行链路中任务存在性检查、终态清理与重复入队防护。
 */

import { beforeEach, describe, expect, it, vi } from 'vitest';

const getJob = vi.fn();
const info = vi.fn();
const warn = vi.fn();

vi.mock('../../infra/redis', () => ({
  cloneMediaDownloadQueue: {
    getJob,
  },
}));

vi.mock('../../logger', () => ({
  logger: {
    info,
    warn,
  },
}));

import {
  buildCloneDownloadJobId,
  hasCloneDownloadJobInFlight,
  prepareCloneDownloadJobForEnqueue,
} from './clone-download-queue.service';

describe('clone-download-queue.service', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    getJob.mockResolvedValue(null);
  });

  it('builds canonical clone download job ids', () => {
    expect(buildCloneDownloadJobId(123)).toBe('clone-download-item-123');
  });

  it('treats waiting jobs as inflight', async () => {
    getJob.mockResolvedValue({
      getState: vi.fn().mockResolvedValue('waiting'),
    });

    await expect(hasCloneDownloadJobInFlight('123')).resolves.toBe(true);
  });

  it('allows enqueue when no existing job is found', async () => {
    await expect(
      prepareCloneDownloadJobForEnqueue({
        itemId: '123',
        source: 'test',
      }),
    ).resolves.toEqual({
      jobId: 'clone-download-item-123',
      canEnqueue: true,
      previousState: null,
    });
  });

  it('blocks enqueue when an existing job is still active', async () => {
    getJob.mockResolvedValue({
      getState: vi.fn().mockResolvedValue('active'),
    });

    await expect(
      prepareCloneDownloadJobForEnqueue({
        itemId: '123',
        source: 'test',
      }),
    ).resolves.toEqual({
      jobId: 'clone-download-item-123',
      canEnqueue: false,
      reason: 'existing_non_terminal',
      existingState: 'active',
    });
  });

  it('removes terminal jobs before re-enqueue', async () => {
    const remove = vi.fn().mockResolvedValue(undefined);
    getJob.mockResolvedValue({
      getState: vi.fn().mockResolvedValue('completed'),
      remove,
    });

    await expect(
      prepareCloneDownloadJobForEnqueue({
        itemId: '123',
        source: 'test',
      }),
    ).resolves.toEqual({
      jobId: 'clone-download-item-123',
      canEnqueue: true,
      previousState: 'completed',
    });

    expect(remove).toHaveBeenCalledTimes(1);
    expect(info).toHaveBeenCalledTimes(1);
  });

  it('reports cleanup failures for terminal jobs', async () => {
    getJob.mockResolvedValue({
      getState: vi.fn().mockResolvedValue('failed'),
      remove: vi.fn().mockRejectedValue(new Error('remove failed')),
    });

    await expect(
      prepareCloneDownloadJobForEnqueue({
        itemId: '123',
        source: 'test',
      }),
    ).resolves.toEqual({
      jobId: 'clone-download-item-123',
      canEnqueue: false,
      reason: 'terminal_remove_failed',
      existingState: 'failed',
    });

    expect(warn).toHaveBeenCalledTimes(1);
  });
});

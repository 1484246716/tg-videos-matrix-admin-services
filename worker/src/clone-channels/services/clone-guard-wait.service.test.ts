/**
 * Clone Channels guard-wait 服务测试：验证 guard 暂停后的恢复与重入队行为。
 * 覆盖状态迁移守卫、按频道公平出队以及下载队列再投递逻辑。
 */

import { beforeEach, describe, expect, it, vi } from 'vitest';

const add = vi.fn();
const updateMany = vi.fn();
const findUnique = vi.fn();
const enqueueGuardWaitJobByChannel = vi.fn();
const dequeueNextGuardWaitJobRoundRobin = vi.fn();
const getGuardWaitFairnessSnapshot = vi.fn();
const prepareCloneDownloadJobForEnqueue = vi.fn();
const info = vi.fn();
const warn = vi.fn();

vi.mock('../../infra/redis', () => ({
  cloneMediaDownloadQueue: {
    add,
  },
}));

vi.mock('../../infra/prisma', () => ({
  prisma: {
    cloneCrawlItem: {
      updateMany,
      findUnique,
    },
  },
}));

vi.mock('./clone-guard-wait-fairness.service', () => ({
  enqueueGuardWaitJobByChannel,
  dequeueNextGuardWaitJobRoundRobin,
  getGuardWaitFairnessSnapshot,
}));

vi.mock('./clone-download-queue.service', () => ({
  prepareCloneDownloadJobForEnqueue,
}));

vi.mock('../../logger', () => ({
  logger: {
    info,
    warn,
  },
}));

import { processCloneGuardWait } from './clone-guard-wait.service';

describe('clone-guard-wait.service', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    getGuardWaitFairnessSnapshot.mockResolvedValue({ channelCount: 1, topChannels: [] });
    dequeueNextGuardWaitJobRoundRobin.mockResolvedValue({
      channelUsername: 'klchigua',
      payload: {
        itemId: '6',
        channelUsername: 'klchigua',
        retryCount: 0,
      },
      remaining: 0,
    });
    prepareCloneDownloadJobForEnqueue.mockResolvedValue({
      jobId: 'clone-download-item-6',
      canEnqueue: true,
      previousState: null,
    });
    updateMany.mockResolvedValue({ count: 1 });
    findUnique.mockResolvedValue(null);
    add.mockResolvedValue(undefined);
  });

  it('transitions paused_by_guard items back to queued before requeueing', async () => {
    await processCloneGuardWait({
      itemId: '6',
      channelUsername: 'klchigua',
      retryCount: 0,
    } as any);

    expect(updateMany).toHaveBeenCalledWith({
      where: {
        id: BigInt(6),
        downloadStatus: {
          in: ['paused_by_guard', 'queued', 'failed_retryable', 'none'],
        },
      },
      data: {
        downloadStatus: 'queued',
        downloadLeaseUntil: null,
        downloadHeartbeatAt: null,
        downloadWorkerJobId: null,
        downloadErrorCode: null,
        downloadError: null,
      },
    });

    expect(add).toHaveBeenCalledTimes(1);
  });

  it('does not requeue when state transition guard rejects the item', async () => {
    updateMany.mockResolvedValue({ count: 0 });
    findUnique.mockResolvedValue({
      downloadStatus: 'downloaded',
      localPath: 'D:\\done.mp4',
    });

    await processCloneGuardWait({
      itemId: '6',
      channelUsername: 'klchigua',
      retryCount: 0,
    } as any);

    expect(add).not.toHaveBeenCalled();
    expect(info).toHaveBeenCalledWith(
      '[clone][guard-wait] skip requeue due to state transition guard',
      expect.objectContaining({
        itemId: '6',
        currentStatus: 'downloaded',
      }),
    );
  });
});

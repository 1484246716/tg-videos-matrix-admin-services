import { MAX_SCHEDULE_BATCH } from '../config/env';
import { prisma } from '../infra/prisma';
import { massMessageQueue } from '../infra/redis';
import { logger } from '../logger';

const DUE_STATUSES = ['pending', 'scheduled', 'failed'] as const;

export async function scheduleDueMassMessageItems() {
  const now = new Date();

  const dueItems = await prisma.massMessageItem.findMany({
    where: {
      status: { in: DUE_STATUSES as any },
      nextRunAt: { lte: now },
    },
    orderBy: [{ nextRunAt: 'asc' }, { id: 'asc' }],
    take: MAX_SCHEDULE_BATCH,
    select: {
      id: true,
      status: true,
      campaignId: true,
    },
  });

  let queued = 0;

  for (const item of dueItems) {
    const updated = await prisma.massMessageItem.updateMany({
      where: {
        id: item.id,
        status: { in: DUE_STATUSES as any },
      },
      data: { status: 'scheduled' as any },
    });

    if (updated.count === 0) continue;

    await massMessageQueue.add(
      'mass-message-send',
      { itemId: item.id.toString() },
      {
        jobId: `mass-message-item-${item.id.toString()}`,
        removeOnComplete: true,
        removeOnFail: 200,
      },
    );

    queued += 1;
  }

  if (queued > 0) {
    logger.info('[scheduler] 已入队群发任务', { count: queued });
  }
}


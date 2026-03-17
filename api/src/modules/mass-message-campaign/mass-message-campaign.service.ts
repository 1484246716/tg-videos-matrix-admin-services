import {
  BadRequestException,
  Injectable,
  InternalServerErrorException,
  NotFoundException,
} from '@nestjs/common';
import { MassMessageCampaignStatus, MassMessageScheduleType } from '@prisma/client';
import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import { PrismaService } from '../prisma/prisma.service';
import { CreateMassMessageCampaignDto } from './dto/create-mass-message-campaign.dto';

let massMessageQueue: Queue | null = null;

function getMassMessageQueue() {
  if (massMessageQueue) return massMessageQueue;

  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
  const connection = new IORedis(redisUrl, { maxRetriesPerRequest: null });
  massMessageQueue = new Queue('q_mass_message', {
    connection: connection as any,
  });

  return massMessageQueue;
}

function parseIsoDateOrThrow(value: string, fieldName: string): Date {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) {
    throw new BadRequestException(`${fieldName} must be a valid ISO date string`);
  }
  return d;
}

@Injectable()
export class MassMessageCampaignService {
  constructor(private readonly prisma: PrismaService) {}

  private get campaignModel() {
    const model = this.prisma.massMessageCampaign;
    if (!model) {
      throw new InternalServerErrorException(
        'Prisma massMessageCampaign model is unavailable. Please run prisma generate and restart API.',
      );
    }
    return model;
  }

  private serializeBigInt<T>(value: T): T {
    return JSON.parse(
      JSON.stringify(value, (_key, v) => (typeof v === 'bigint' ? v.toString() : v)),
    ) as T;
  }

  async list(params: { status?: string; limit?: number; userId?: string; role?: string }) {
    const rows = await this.campaignModel.findMany({
      where: {
        status: params.status as MassMessageCampaignStatus | undefined,
        createdBy:
          params.role === 'admin'
            ? undefined
            : params.userId
              ? BigInt(params.userId)
              : undefined,
      },
      orderBy: { createdAt: 'desc' },
      take: params.limit ?? 100,
    });
    return this.serializeBigInt(rows);
  }

  async getOne(id: string, userId?: string, role?: string) {
    const row = await this.campaignModel.findFirst({
      where:
        role === 'admin'
          ? { id: BigInt(id) }
          : { id: BigInt(id), createdBy: userId ? BigInt(userId) : undefined },
      include: { items: { orderBy: { createdAt: 'desc' }, take: 200 } },
    });
    if (!row) throw new NotFoundException('mass message campaign not found');
    return this.serializeBigInt(row);
  }

  async create(dto: CreateMassMessageCampaignDto, userId?: string, role?: string) {
    if (!dto.name.trim()) throw new BadRequestException('name is required');
    if (!dto.targetIds || dto.targetIds.length === 0) {
      throw new BadRequestException('targetIds is required');
    }
    if (!dto.templateId && !dto.contentOverride) {
      throw new BadRequestException('templateId or contentOverride is required');
    }

    if (dto.scheduleType === 'recurring' && !dto.scheduledAt) {
      throw new BadRequestException('scheduledAt is required for recurring');
    }

    const now = new Date();
    const timezone = dto.timezone ?? 'Asia/Shanghai';

    const scheduleType = dto.scheduleType as MassMessageScheduleType;
    const scheduledAt =
      dto.scheduleType === 'scheduled'
        ? parseIsoDateOrThrow(dto.scheduledAt!, 'scheduledAt')
        : dto.scheduledAt
          ? parseIsoDateOrThrow(dto.scheduledAt, 'scheduledAt')
          : null;

    const firstRunAt =
      scheduleType === 'immediate'
        ? now
        : scheduleType === 'scheduled'
          ? scheduledAt!
          : // recurring: use scheduledAt if provided, otherwise run immediately
            scheduledAt ?? now;

    const retryCount = dto.retryCount ?? 3;
    const retryIntervalSec = dto.retryIntervalSec ?? 30;
    const rateLimitPerMin = dto.rateLimitPerMin ?? 10;
    const pinMode = dto.pinMode ?? 'none';

    const created = await this.prisma.massMessageCampaign.create({
      data: {
        name: dto.name.trim(),
        status: MassMessageCampaignStatus.queued,
        templateId: dto.templateId ? BigInt(dto.templateId) : undefined,
        contentOverride: dto.contentOverride,
        formatOverride: dto.formatOverride as any,
        imageUrlOverride: dto.imageUrlOverride,
        buttonsOverride: dto.buttonsOverride as any,
        targetType: dto.targetType,
        targetIds: dto.targetIds,
        scheduleType: scheduleType as any,
        timezone,
        scheduledAt,
        recurringPattern: dto.recurringPattern as any,
        rateLimitPerMin,
        retryCount,
        retryIntervalSec,
        pinMode: pinMode as any,
        progressTotal: dto.targetIds.length,
        createdBy: role === 'admin' ? null : userId ? BigInt(userId) : null,
        items: {
          create: dto.targetIds.map((targetId) => ({
            targetId,
            targetType: dto.targetType === 'mixed' ? 'channel' : dto.targetType,
            status: 'pending',
            nextRunAt: firstRunAt,
            maxRetries: retryCount,
            plannedAt: firstRunAt,
          })),
        },
      },
      include: {
        items: { select: { id: true } },
      },
    });

    if (scheduleType === 'immediate') {
      // Immediately enqueue items so UI "测试发送" can trigger sending without relying on DB polling.
      // Worker will still update statuses and handle retries.
      try {
        const queue = getMassMessageQueue();
        for (const item of created.items ?? []) {
          await queue.add(
            'mass-message-send',
            { itemId: item.id.toString() },
            {
              jobId: `mass-message-item-${item.id.toString()}`,
              removeOnComplete: true,
              removeOnFail: 200,
            },
          );
        }
      } catch {
        // If enqueue fails, scheduler can still pick it up later. Do not fail the API call.
      }
    }

    return this.serializeBigInt(created);
  }
}


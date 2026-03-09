import { Injectable } from '@nestjs/common';
import { Prisma, RiskLevel } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';

type RiskEventListParams = {
  eventType?: string;
  level?: RiskLevel;
  channelId?: string;
  botId?: string;
  dispatchTaskId?: string;
  from?: string;
  to?: string;
  limit?: number;
};

@Injectable()
export class RiskEventService {
  constructor(private readonly prisma: PrismaService) {}

  async list(params: RiskEventListParams) {
    const where: Prisma.RiskEventWhereInput = {
      eventType: params.eventType,
      level: params.level,
      channelId: params.channelId ? BigInt(params.channelId) : undefined,
      botId: params.botId ? BigInt(params.botId) : undefined,
      dispatchTaskId: params.dispatchTaskId
        ? BigInt(params.dispatchTaskId)
        : undefined,
      createdAt:
        params.from || params.to
          ? {
              gte: params.from ? new Date(params.from) : undefined,
              lte: params.to ? new Date(params.to) : undefined,
            }
          : undefined,
    };

    return this.prisma.riskEvent.findMany({
      where,
      orderBy: { createdAt: 'desc' },
      take: params.limit ?? 100,
    });
  }
}

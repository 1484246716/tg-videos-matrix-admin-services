import { Injectable, InternalServerErrorException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class MassMessageItemService {
  constructor(private readonly prisma: PrismaService) {}

  private get model() {
    const model = this.prisma.massMessageItem;
    if (!model) {
      throw new InternalServerErrorException(
        'Prisma massMessageItem model is unavailable. Please run prisma generate and restart API.',
      );
    }
    return model;
  }

  private serializeBigInt<T>(value: T): T {
    return JSON.parse(
      JSON.stringify(value, (_key, v) => (typeof v === 'bigint' ? v.toString() : v)),
    ) as T;
  }

  async list(params: { campaignId?: string; status?: string; limit?: number; offset?: number }) {
    const rows = await this.model.findMany({
      where: {
        campaignId: params.campaignId ? BigInt(params.campaignId) : undefined,
        status: params.status as any,
      },
      orderBy: [{ createdAt: 'desc' }],
      take: params.limit ?? 200,
      skip: params.offset ?? 0,
    });
    return this.serializeBigInt(rows);
  }
}


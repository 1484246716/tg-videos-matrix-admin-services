import {
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { PrismaClientKnownRequestError } from '@prisma/client/runtime/library';
import { PrismaService } from '../prisma/prisma.service';
import { CreateRelayChannelDto } from './dto/create-relay-channel.dto';
import { UpdateRelayChannelDto } from './dto/update-relay-channel.dto';

@Injectable()
export class RelayChannelService {
  constructor(private readonly prisma: PrismaService) {}

  private serializeBigInt<T>(value: T): T {
    return JSON.parse(
      JSON.stringify(value, (_key, v) =>
        typeof v === 'bigint' ? v.toString() : v,
      ),
    ) as T;
  }

  async list() {
    const rows = await this.prisma.relayChannel.findMany({
      orderBy: { createdAt: 'desc' },
      include: {
        bot: {
          select: {
            id: true,
            name: true,
            status: true,
            username: true,
          },
        },
      },
    });

    return this.serializeBigInt(rows);
  }

  async create(dto: CreateRelayChannelDto) {
    try {
      const created = await this.prisma.relayChannel.create({
        data: {
          name: dto.name,
          tgChatId: BigInt(dto.tgChatId),
          botId: dto.botId,
          isActive: dto.isActive ?? true,
          autoCleanupDays: dto.autoCleanupDays ?? 30,
        },
        include: {
          bot: {
            select: {
              id: true,
              name: true,
              status: true,
              username: true,
            },
          },
        },
      });

      return this.serializeBigInt(created);
    } catch (error) {
      if (
        error instanceof PrismaClientKnownRequestError &&
        error.code === 'P2002'
      ) {
        throw new ConflictException('中转频道 ChatId 已存在，请勿重复创建');
      }
      throw error;
    }
  }

  async getOne(id: string) {
    const item = await this.prisma.relayChannel.findUnique({
      where: { id: BigInt(id) },
      include: {
        bot: {
          select: {
            id: true,
            name: true,
            status: true,
            username: true,
          },
        },
      },
    });

    if (!item) {
      throw new NotFoundException('relayChannel not found');
    }

    return this.serializeBigInt(item);
  }

  async update(id: string, dto: UpdateRelayChannelDto) {
    await this.getOne(id);

    try {
      const updated = await this.prisma.relayChannel.update({
        where: { id: BigInt(id) },
        data: {
          name: dto.name,
          tgChatId: dto.tgChatId ? BigInt(dto.tgChatId) : undefined,
          botId: dto.botId,
          isActive: dto.isActive,
          autoCleanupDays: dto.autoCleanupDays,
        },
        include: {
          bot: {
            select: {
              id: true,
              name: true,
              status: true,
              username: true,
            },
          },
        },
      });

      return this.serializeBigInt(updated);
    } catch (error) {
      if (
        error instanceof PrismaClientKnownRequestError &&
        error.code === 'P2002'
      ) {
        throw new ConflictException('中转频道 ChatId 已存在，请使用其他 ChatId');
      }
      throw error;
    }
  }

  async remove(id: string) {
    await this.getOne(id);

    const deleted = await this.prisma.relayChannel.delete({
      where: { id: BigInt(id) },
    });

    return this.serializeBigInt(deleted);
  }
}

import { BadRequestException, Injectable } from '@nestjs/common';
import { MediaStatus, Prisma, TaskStatus } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { CreateDispatchTaskDto } from './dto/create-dispatch-task.dto';
import { UpdateDispatchTaskStatusDto } from './dto/update-dispatch-task-status.dto';

@Injectable()
export class DispatchService {
  constructor(private readonly prisma: PrismaService) {}

  async list(params: {
    channelId?: string;
    status?: TaskStatus;
    limit?: number;
  }) {
    return this.prisma.dispatchTask.findMany({
      where: {
        channelId: params.channelId ? BigInt(params.channelId) : undefined,
        status: params.status,
      },
      orderBy: [{ priority: 'asc' }, { nextRunAt: 'asc' }],
      take: params.limit ?? 100,
      include: {
        channel: {
          select: {
            id: true,
            name: true,
            tgChatId: true,
          },
        },
        mediaAsset: {
          select: {
            id: true,
            originalName: true,
            telegramFileId: true,
            status: true,
          },
        },
      },
    });
  }

  async create(dto: CreateDispatchTaskDto) {
    const scheduleSlot = new Date(dto.scheduleSlot);
    const plannedAt = new Date(dto.plannedAt);
    const nextRunAt = new Date(dto.nextRunAt);

    if (
      Number.isNaN(scheduleSlot.getTime()) ||
      Number.isNaN(plannedAt.getTime()) ||
      Number.isNaN(nextRunAt.getTime())
    ) {
      throw new BadRequestException(
        'Invalid datetime input. scheduleSlot/plannedAt/nextRunAt must be ISO datetime.',
      );
    }

    const mediaAsset = await this.prisma.mediaAsset.findUnique({
      where: { id: BigInt(dto.mediaAssetId) },
      select: {
        id: true,
        channelId: true,
        status: true,
        telegramFileId: true,
      },
    });

    if (!mediaAsset) {
      throw new BadRequestException('mediaAsset not found');
    }

    if (mediaAsset.channelId !== BigInt(dto.channelId)) {
      throw new BadRequestException('mediaAsset does not belong to channel');
    }

    if (
      mediaAsset.status !== MediaStatus.relay_uploaded ||
      !mediaAsset.telegramFileId
    ) {
      throw new BadRequestException(
        'mediaAsset is not ready for dispatch. Require relay_uploaded with telegramFileId.',
      );
    }

    try {
      return await this.prisma.dispatchTask.create({
        data: {
          channelId: BigInt(dto.channelId),
          mediaAssetId: BigInt(dto.mediaAssetId),
          groupKey: dto.groupKey?.trim() || undefined,
          botId: dto.botId ? BigInt(dto.botId) : undefined,
          scheduleSlot,
          plannedAt,
          nextRunAt,
          priority: dto.priority ?? 100,
          maxRetries: dto.maxRetries ?? 6,
        },
        include: {
          channel: {
            select: {
              id: true,
              name: true,
            },
          },
          mediaAsset: {
            select: {
              id: true,
              originalName: true,
            },
          },
        },
      });
    } catch (error) {
      if (error instanceof Prisma.PrismaClientKnownRequestError && error.code === 'P2002') {
        throw new BadRequestException('dispatch task duplicated by channel/schedule/groupKey constraint');
      }
      throw error;
    }
  }

  async updateStatus(id: string, dto: UpdateDispatchTaskStatusDto) {
    const now = new Date();

    const data: Prisma.DispatchTaskUpdateInput = {
      status: dto.status,
      telegramErrorCode: dto.telegramErrorCode,
      telegramErrorMessage: dto.telegramErrorMessage,
      telegramMessageId: dto.telegramMessageId
        ? BigInt(dto.telegramMessageId)
        : undefined,
      telegramMessageLink: dto.telegramMessageLink,
    };

    if (dto.status === 'running') {
      data.startedAt = now;
    }

    if (['success', 'failed', 'cancelled', 'dead'].includes(dto.status)) {
      data.finishedAt = now;
    }

    return this.prisma.dispatchTask.update({
      where: { id: BigInt(id) },
      data,
    });
  }

  async listLogs(dispatchTaskId: string, limit?: number) {
    return this.prisma.dispatchTaskLog.findMany({
      where: { dispatchTaskId: BigInt(dispatchTaskId) },
      orderBy: { createdAt: 'desc' },
      take: limit ?? 100,
    });
  }
}

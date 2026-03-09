import { Injectable } from '@nestjs/common';
import { MediaStatus, Prisma } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { CreateMediaAssetDto } from './dto/create-media-asset.dto';
import { UpdateMediaAssetStatusDto } from './dto/update-media-asset-status.dto';
import { MarkRelayUploadedDto } from './dto/mark-relay-uploaded.dto';

@Injectable()
export class MediaAssetService {
  constructor(private readonly prisma: PrismaService) {}

  async list(params: { channelId?: string; status?: MediaStatus; limit?: number }) {
    return this.prisma.mediaAsset.findMany({
      where: {
        channelId: params.channelId ? BigInt(params.channelId) : undefined,
        status: params.status,
      },
      orderBy: { createdAt: 'desc' },
      take: params.limit ?? 50,
      include: {
        channel: {
          select: {
            id: true,
            name: true,
            tgChatId: true,
          },
        },
      },
    });
  }

  async create(dto: CreateMediaAssetDto) {
    return this.prisma.mediaAsset.create({
      data: {
        channelId: BigInt(dto.channelId),
        originalName: dto.originalName,
        localPath: dto.localPath,
        fileSize: BigInt(dto.fileSize),
        fileHash: dto.fileHash,
        durationSec: dto.durationSec,
      },
      include: {
        channel: {
          select: {
            id: true,
            name: true,
          },
        },
      },
    });
  }

  async updateStatus(id: string, dto: UpdateMediaAssetStatusDto) {
    const data: Prisma.MediaAssetUpdateInput = {
      status: dto.status,
      ingestError: dto.ingestError,
    };

    return this.prisma.mediaAsset.update({
      where: { id: BigInt(id) },
      data,
    });
  }

  async markRelayUploaded(id: string, dto: MarkRelayUploadedDto) {
    return this.prisma.mediaAsset.update({
      where: { id: BigInt(id) },
      data: {
        status: MediaStatus.relay_uploaded,
        telegramFileId: dto.telegramFileId,
        telegramFileUniqueId: dto.telegramFileUniqueId,
        relayMessageId: BigInt(dto.relayMessageId),
        ingestError: null,
      },
    });
  }
}

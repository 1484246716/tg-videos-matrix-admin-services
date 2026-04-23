import {
  BadRequestException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { Queue } from 'bullmq';
import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { readdir, stat } from 'node:fs/promises';
import { basename, extname, resolve } from 'node:path';
import IORedis from 'ioredis';
import { MediaStatus, Prisma } from '@prisma/client';
import { ContentTaxonomyService } from '../content-taxonomy/content-taxonomy.service';
import { PrismaService } from '../prisma/prisma.service';
import { CreateMediaAssetDto } from './dto/create-media-asset.dto';
import { UpdateMediaAssetStatusDto } from './dto/update-media-asset-status.dto';
import { MarkRelayUploadedDto } from './dto/mark-relay-uploaded.dto';
import { BatchEnqueueRelayUploadDto } from './dto/batch-enqueue-relay-upload.dto';

const SUPPORTED_VIDEO_EXT = new Set([
  '.mp4',
  '.mkv',
  '.mov',
  '.avi',
  '.m4v',
  '.webm',
]);

let searchIndexQueue: Queue | null = null;

function getSearchIndexQueue() {
  if (searchIndexQueue) return searchIndexQueue;

  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
  const connection = new IORedis(redisUrl, { maxRetriesPerRequest: null });
  searchIndexQueue = new Queue('q_search_index', {
    connection: connection as any,
  });
  return searchIndexQueue;
}

@Injectable()
export class MediaAssetService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly contentTaxonomyService: ContentTaxonomyService,
  ) {}

  async list(params: {
    channelId?: string;
    status?: MediaStatus;
    keyword?: string;
    limit?: number;
    page?: number;
    pageSize?: number;
    userId?: string;
    role?: string;
  }) {
    const keyword = (params.keyword || '').trim();
    const where = {
      channelId: params.channelId ? BigInt(params.channelId) : undefined,
      status: params.status,
      originalName: keyword
        ? {
            contains: keyword,
            mode: 'insensitive' as const,
          }
        : undefined,
      channel:
        params.role === 'admin'
          ? undefined
          : {
              createdBy: params.userId ? BigInt(params.userId) : undefined,
            },
    };

    const usePagination = Number.isFinite(params.page) || Number.isFinite(params.pageSize);
    const pageSize = Math.max(1, Math.min(200, Math.floor(params.pageSize ?? params.limit ?? 50)));
    const page = Math.max(1, Math.floor(params.page ?? 1));
    const skip = (page - 1) * pageSize;

    const [total, items] = await this.prisma.$transaction([
      this.prisma.mediaAsset.count({ where }),
      this.prisma.mediaAsset.findMany({
        where,
        orderBy: { createdAt: 'desc' },
        ...(usePagination ? { skip, take: pageSize } : { take: params.limit ?? 50 }),
        include: {
          channel: {
            select: {
              id: true,
              name: true,
              tgChatId: true,
            },
          },
          categories: {
            include: {
              level2: {
                include: {
                  level1: true,
                },
              },
            },
          },
          tags: {
            include: {
              tag: true,
            },
          },
        },
      }),
    ]);

    const list = items.map((item) => ({
      ...item,
      id: item.id.toString(),
      channelId: item.channelId.toString(),
      fileSize: item.fileSize.toString(),
      ingestDurationSec: item.ingestDurationSec ?? null,
      ingestStartedAt: item.ingestStartedAt ?? null,
      ingestFinishedAt: item.ingestFinishedAt ?? null,
      relayMessageId: item.relayMessageId ? item.relayMessageId.toString() : null,
      channel: item.channel
        ? {
            ...item.channel,
            id: item.channel.id.toString(),
          }
        : null,
      categories: item.categories.map((category) => ({
        id: category.level2.id.toString(),
        name: category.level2.name,
        slug: category.level2.slug,
        level1Id: category.level2.level1Id.toString(),
        level1Name: category.level2.level1.name,
        level1Slug: category.level2.level1.slug,
      })),
      tags: item.tags.map((tag) => ({
        id: tag.tag.id.toString(),
        name: tag.tag.name,
        slug: tag.tag.slug,
        scope: tag.tag.scope,
      })),
    }));

    if (!usePagination) {
      return list;
    }

    return {
      list,
      pagination: {
        page,
        pageSize,
        total,
        totalPages: Math.max(1, Math.ceil(total / pageSize)),
      },
    };
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

  async updateTaxonomy(
    id: string,
    dto: { level2Ids?: string[]; tagIds?: string[] },
    userId?: string,
    role?: string,
  ) {
    const asset = await this.prisma.mediaAsset.findFirst({
      where: {
        id: BigInt(id),
        channel:
          role === 'admin'
            ? undefined
            : {
                createdBy: userId ? BigInt(userId) : undefined,
              },
      },
      select: {
        id: true,
        collectionEpisode: {
          select: { id: true },
        },
      },
    });

    if (!asset) {
      throw new NotFoundException('media asset not found');
    }

    const taxonomy = await this.contentTaxonomyService.replaceMediaAssetTaxonomy(asset.id, dto);
    await this.enqueueSearchIndexJob({
      sourceType: asset.collectionEpisode ? 'collection_episode' : 'media_asset',
      sourceId: asset.collectionEpisode ? asset.collectionEpisode.id.toString() : asset.id.toString(),
      mediaAssetId: asset.id.toString(),
    });

    return {
      mediaAssetId: asset.id.toString(),
      ...taxonomy,
    };
  }

  private async hashFile(filePath: string): Promise<string> {
    return new Promise((resolveHash, reject) => {
      const hash = createHash('sha256');
      const stream = createReadStream(filePath);

      stream.on('data', (chunk) => hash.update(chunk));
      stream.on('end', () => resolveHash(hash.digest('hex')));
      stream.on('error', reject);
    });
  }

  private async scanChannelVideos(folderPath: string) {
    const absolute = resolve(folderPath);
    const entries = await readdir(absolute, { withFileTypes: true });

    const files = entries
      .filter((entry) => entry.isFile())
      .map((entry) => resolve(absolute, entry.name))
      .filter((filePath) => SUPPORTED_VIDEO_EXT.has(extname(filePath).toLowerCase()));

    return files;
  }

  async batchEnqueueRelayUpload(dto: BatchEnqueueRelayUploadDto) {
    const relayChannel = await this.prisma.relayChannel.findUnique({
      where: { id: BigInt(dto.relayChannelId) },
      select: { id: true, isActive: true },
    });

    if (!relayChannel) {
      throw new NotFoundException('relay channel not found');
    }

    if (!relayChannel.isActive) {
      throw new BadRequestException('relay channel is not active');
    }

    const channels = await this.prisma.channel.findMany({
      where: { status: 'active' },
      select: { id: true, folderPath: true },
    });

    let scannedFiles = 0;
    let createdAssets = 0;
    let enqueuedTasks = 0;

    for (const channel of channels) {
      let files: string[] = [];
      try {
        files = await this.scanChannelVideos(channel.folderPath);
      } catch {
        continue;
      }

      for (const filePath of files) {
        scannedFiles += 1;

        const s = await stat(filePath);
        const fileHash = await this.hashFile(filePath);

        let asset = await this.prisma.mediaAsset.findUnique({
          where: {
            fileHash_fileSize: {
              fileHash,
              fileSize: s.size,
            },
          },
          select: { id: true, status: true },
        });

        if (!asset) {
          asset = await this.prisma.mediaAsset.create({
            data: {
              channelId: channel.id,
              originalName: basename(filePath),
              localPath: filePath,
              fileSize: BigInt(s.size),
              fileHash,
              status: MediaStatus.new,
            },
            select: { id: true, status: true },
          });
          createdAssets += 1;
        }

        if (asset.status === MediaStatus.relay_uploaded) {
          continue;
        }

        await this.prisma.mediaAsset.update({
          where: { id: asset.id },
          data: {
            status: MediaStatus.ready,
            sourceMeta: {
              relayChannelId: dto.relayChannelId,
              relayEnqueueAt: new Date().toISOString(),
              relayPriority: dto.priority ?? 100,
              relayMaxRetries: dto.maxRetries ?? 6,
            },
          },
        });

        enqueuedTasks += 1;
      }
    }

    return {
      relayChannelId: dto.relayChannelId,
      scannedFiles,
      createdAssets,
      enqueuedTasks,
      message: 'Type A 扫描并入队完成（当前入 dispatch_tasks 队列）',
    };
  }

  private async enqueueSearchIndexJob(payload: {
    sourceType: 'media_asset' | 'collection_episode';
    sourceId: string;
    mediaAssetId: string;
  }) {
    const queue = getSearchIndexQueue();
    await queue.add(
      'upsert',
      {
        sourceType: payload.sourceType,
        sourceId: payload.sourceId,
        mediaAssetId: payload.mediaAssetId,
      },
      {
        jobId: `search-index-${payload.sourceType}-${payload.sourceId}`,
        removeOnComplete: true,
        removeOnFail: 200,
      },
    );
  }
}

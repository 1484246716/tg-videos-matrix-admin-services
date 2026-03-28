import { BadRequestException, Injectable, InternalServerErrorException, NotFoundException } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import { mkdir, rm } from 'node:fs/promises';
import { normalize, resolve } from 'node:path';
import { PrismaService } from '../prisma/prisma.service';

type SaveCollectionDto = {
  channelId: string;
  name: string;
  slug?: string;
  dirPath: string;
  status: 'active' | 'paused' | 'archived';
  sortOrder: number;
  navEnabled: boolean;
  navPageSize: number;
  templateText?: string;
};

function parseCollectionMeta(sourceMeta: unknown) {
  if (!sourceMeta || typeof sourceMeta !== 'object') return null;
  const meta = sourceMeta as Record<string, unknown>;
  if (meta.isCollection !== true) return null;

  const collectionName = typeof meta.collectionName === 'string' ? meta.collectionName.trim() : '';
  const episodeNo =
    typeof meta.episodeNo === 'number'
      ? meta.episodeNo
      : typeof meta.episodeNo === 'string' && /^\d+$/.test(meta.episodeNo)
        ? Number(meta.episodeNo)
        : null;

  if (!collectionName || episodeNo === null) return null;

  return {
    collectionName,
    episodeNo,
  };
}

function getFileStem(name: string) {
  if (!name) return '';
  const base = name.replace(/^.*[\\/]/, '');
  const idx = base.lastIndexOf('.');
  return idx > 0 ? base.slice(0, idx) : base;
}

let searchIndexQueue: Queue | null = null;

function getSearchIndexQueue() {
  if (searchIndexQueue) return searchIndexQueue;

  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
  const connection = new IORedis(redisUrl, { maxRetriesPerRequest: null });
  connection.on('error', (error) => {
    console.error('[redis] collection search-index redis error:', error?.message ?? error);
  });

  searchIndexQueue = new Queue('q_search_index', {
    connection: connection as any,
  });

  return searchIndexQueue;
}

@Injectable()
export class CollectionService {
  constructor(private readonly prisma: PrismaService) {}

  private getChannelsRootDir() {
    const raw = (process.env.CHANNELS_ROOT_DIR || './data/channels').trim();

    if (/^\/[a-zA-Z]/.test(raw)) {
      const driveRelative = raw.replace(/^\//, '');
      const workspaceRoot = resolve(process.cwd(), '..', '..');
      return resolve(workspaceRoot, driveRelative);
    }

    return resolve(raw);
  }

  private resolveChannelRelativePath(folderPath: string) {
    const normalizedInput = normalize(folderPath.trim().replace(/\\/g, '/'));
    return normalizedInput.replace(/^[\\/]+/, '');
  }

  private resolveCollectionFolderPath(args: { channelFolderPath: string; collectionName: string }) {
    const channelsRoot = this.getChannelsRootDir();
    const channelRel = this.resolveChannelRelativePath(args.channelFolderPath);
    return resolve(channelsRoot, channelRel, 'Collection', args.collectionName);
  }

  private async ensureCollectionFolderExists(args: { channelFolderPath: string; collectionName: string }) {
    const targetDir = this.resolveCollectionFolderPath(args);
    await mkdir(targetDir, { recursive: true });
    return targetDir;
  }

  private async removeCollectionFolderIfExists(args: { channelFolderPath: string; collectionName: string }) {
    const targetDir = this.resolveCollectionFolderPath(args);
    await rm(targetDir, { recursive: true, force: true });
    return targetDir;
  }

  private toResponse(row: any) {
    const episodes = Array.isArray(row.episodes) ? row.episodes : [];
    const now = Date.now();

    const blockedEpisode = episodes
      .map((ep: any) => {
        const sourceMeta = ep?.mediaAsset?.sourceMeta && typeof ep.mediaAsset.sourceMeta === 'object'
          ? (ep.mediaAsset.sourceMeta as Record<string, unknown>)
          : {};
        const skipStatus = typeof sourceMeta.skipStatus === 'string' ? sourceMeta.skipStatus : null;
        const isSkippedMissing = skipStatus === 'skipped_missing';
        const dispatchTasks = Array.isArray(ep?.mediaAsset?.dispatchTasks) ? ep.mediaAsset.dispatchTasks : [];
        const hasSuccess = dispatchTasks.some((t: any) => t.status === 'success');
        const blocked = !isSkippedMissing && !hasSuccess;
        return {
          episodeNo: ep.episodeNo as number,
          blocked,
          isSkippedMissing,
          skipReason: typeof sourceMeta.skipReason === 'string' ? sourceMeta.skipReason : null,
          skipAt: typeof sourceMeta.skipAt === 'string' ? sourceMeta.skipAt : null,
          mediaUpdatedAt: ep?.mediaAsset?.updatedAt ? new Date(ep.mediaAsset.updatedAt).getTime() : null,
          mediaAssetId: ep?.mediaAsset?.id ? String(ep.mediaAsset.id) : null,
        };
      })
      .filter((item: { blocked: boolean }) => item.blocked)
      .sort(
        (
          a: { episodeNo: number },
          b: { episodeNo: number },
        ) => a.episodeNo - b.episodeNo,
      )[0];

    const blockState = blockedEpisode ? 'blocked' : 'unblocked';
    const waitingPrevEpisodeNo = blockedEpisode?.episodeNo ?? null;
    const blockedDurationSec = blockedEpisode?.mediaUpdatedAt
      ? Math.max(0, Math.floor((now - blockedEpisode.mediaUpdatedAt) / 1000))
      : null;

    return {
      ...row,
      id: row.id.toString(),
      channelId: row.channelId.toString(),
      createdBy: row.createdBy != null ? row.createdBy.toString() : null,
      channel: row.channel
        ? {
            ...row.channel,
            id: row.channel.id.toString(),
          }
        : undefined,
      _count: row._count,
      blockState,
      blockReason: blockedEpisode ? `等待前序集 ${blockedEpisode.episodeNo}` : null,
      waitingPrevEpisodeNo,
      currentEpisodeNo: null,
      blockingTaskId: blockedEpisode?.mediaAssetId ?? null,
      blockedDurationSec,
    };
  }

  async list(userId?: string, role?: string) {
    const where: Prisma.CollectionWhereInput =
      role === 'admin'
        ? {}
        : {
            createdBy: userId ? BigInt(userId) : undefined,
          };

    const rows = await this.prisma.collection.findMany({
      where,
      orderBy: { updatedAt: 'desc' },
      include: {
        channel: { select: { id: true, name: true } },
        _count: { select: { episodes: true } },
        episodes: {
          select: {
            episodeNo: true,
            mediaAsset: {
              select: {
                id: true,
                updatedAt: true,
                sourceMeta: true,
                dispatchTasks: {
                  select: { status: true },
                  where: { status: 'success' },
                  take: 1,
                },
              },
            },
          },
        },
      },
    });

    return rows.map((row) => this.toResponse(row));
  }

  async create(dto: SaveCollectionDto, userId?: string, role?: string) {
    const channel = await this.prisma.channel.findFirst({
      where:
        role === 'admin'
          ? { id: BigInt(dto.channelId) }
          : {
              id: BigInt(dto.channelId),
              createdBy: userId ? BigInt(userId) : undefined,
            },
      select: { id: true, folderPath: true },
    });
    if (!channel) throw new NotFoundException('channel not found');

    await this.ensureCollectionFolderExists({
      channelFolderPath: channel.folderPath,
      collectionName: dto.name,
    });

    const created = await this.prisma.collection.create({
      data: {
        channelId: BigInt(dto.channelId),
        name: dto.name,
        slug: dto.slug || null,
        dirPath: dto.dirPath,
        status: dto.status,
        sortOrder: dto.sortOrder,
        navEnabled: dto.navEnabled,
        navPageSize: dto.navPageSize,
        templateText: dto.templateText || null,
        createdBy: role === 'admin' ? null : userId ? BigInt(userId) : null,
      },
      include: {
        channel: { select: { id: true, name: true } },
        _count: { select: { episodes: true } },
      },
    });

    await this.enqueueSearchIndexJob({
      sourceType: 'collection',
      sourceId: created.id.toString(),
      channelId: created.channelId.toString(),
    });

    return this.toResponse(created);
  }

  async update(id: string, dto: Partial<SaveCollectionDto>, userId?: string, role?: string) {
    const existing = await this.prisma.collection.findFirst({
      where:
        role === 'admin'
          ? { id: BigInt(id) }
          : { id: BigInt(id), createdBy: userId ? BigInt(userId) : undefined },
      select: { id: true, channelId: true, channel: { select: { folderPath: true } } },
    });
    if (!existing) throw new NotFoundException('collection not found');

    if (dto.name || dto.channelId) {
      const channelId = dto.channelId ? BigInt(dto.channelId) : existing.channelId;
      const channel = await this.prisma.channel.findFirst({
        where:
          role === 'admin'
            ? { id: channelId }
            : { id: channelId, createdBy: userId ? BigInt(userId) : undefined },
        select: { id: true, folderPath: true },
      });

      if (!channel) {
        throw new NotFoundException('channel not found');
      }

      const collectionName = (dto.name || '').trim();
      if (collectionName) {
        await this.ensureCollectionFolderExists({
          channelFolderPath: channel.folderPath,
          collectionName,
        });
      }
    }

    const updated = await this.prisma.collection.update({
      where: { id: BigInt(id) },
      data: {
        channelId: dto.channelId ? BigInt(dto.channelId) : undefined,
        name: dto.name,
        slug: dto.slug || undefined,
        dirPath: dto.dirPath,
        status: dto.status,
        sortOrder: dto.sortOrder,
        navEnabled: dto.navEnabled,
        navPageSize: dto.navPageSize,
        templateText: dto.templateText || undefined,
      },
      include: {
        channel: { select: { id: true, name: true } },
        _count: { select: { episodes: true } },
      },
    });

    await this.enqueueSearchIndexJob({
      sourceType: 'collection',
      sourceId: updated.id.toString(),
      channelId: updated.channelId.toString(),
    });

    return this.toResponse(updated);
  }

  private formatCollectionEpisodeTitle(args: {
    episodeNo: number;
    episodeTitle?: string | null;
    sourceTitle?: string | null;
    templateText?: string | null;
  }) {
    const safeTitle = (args.episodeTitle || '').trim();
    if (safeTitle) return safeTitle;

    const fallbackTitle = (args.sourceTitle || '').trim() || `第${args.episodeNo}集`;

    const safeTemplate = (args.templateText || '').trim();
    if (safeTemplate) {
      return safeTemplate
        .replace(/\{episodeNo\}/g, String(args.episodeNo))
        .replace(/\{title\}/g, fallbackTitle);
    }

    return fallbackTitle;
  }

  async getCatalogPreview(
    id: string,
    userId?: string,
    role?: string,
    pagination?: { page?: string; pageSize?: string },
  ) {
    const collection = await this.prisma.collection.findFirst({
      where:
        role === 'admin'
          ? { id: BigInt(id) }
          : { id: BigInt(id), createdBy: userId ? BigInt(userId) : undefined },
      select: {
        id: true,
        name: true,
        templateText: true,
        navPageSize: true,
        channelId: true,
        channel: {
          select: {
            name: true,
            tgChatId: true,
          },
        },
      },
    });

    if (!collection) throw new NotFoundException('collection not found');

    const safePage = Math.max(1, Number.parseInt((pagination?.page || '').trim(), 10) || 1);
    const configuredPageSize = Number(collection.navPageSize ?? 20);
    const safePageSize = Math.min(100, Math.max(1, Number.isFinite(configuredPageSize) ? configuredPageSize : 20));

    const dispatchTasks = await this.prisma.dispatchTask.findMany({
      where: {
        channelId: collection.channelId,
        status: 'success',
        telegramMessageLink: { not: null },
      },
      orderBy: { finishedAt: 'desc' },
      select: {
        id: true,
        mediaAssetId: true,
        telegramMessageLink: true,
        mediaAsset: {
          select: {
            originalName: true,
            sourceMeta: true,
          },
        },
      },
    });

    const episodeOverrideRows = await this.prisma.collectionEpisode.findMany({
      where: {
        collectionId: collection.id,
      },
      select: {
        id: true,
        mediaAssetId: true,
        episodeNo: true,
        episodeTitle: true,
      },
    });

    const overrideByMediaAssetId = new Map(
      episodeOverrideRows.map((row) => [row.mediaAssetId.toString(), row]),
    );

    const mappedEpisodes = dispatchTasks
      .map((task) => {
        const sourceMeta = task.mediaAsset?.sourceMeta;
        const parsed = parseCollectionMeta(sourceMeta);
        if (!parsed) return null;
        if (parsed.collectionName !== collection.name) return null;

        const mediaAssetId = task.mediaAssetId.toString();
        const override = overrideByMediaAssetId.get(mediaAssetId);
        const sourceTitle = getFileStem(task.mediaAsset?.originalName || '');

        return {
          id: override?.id?.toString() || `dispatch-${task.id.toString()}`,
          mediaAssetId,
          episodeNo: parsed.episodeNo,
          title: this.formatCollectionEpisodeTitle({
            episodeNo: parsed.episodeNo,
            episodeTitle: override?.episodeTitle,
            sourceTitle,
            templateText: collection.templateText,
          }),
          link: task.telegramMessageLink || '',
          readonlyLink: task.telegramMessageLink || '',
        };
      })
      .filter((item): item is NonNullable<typeof item> => Boolean(item))
      .sort((a, b) => a.episodeNo - b.episodeNo);

    const total = mappedEpisodes.length;
    const skip = (safePage - 1) * safePageSize;
    const paged = mappedEpisodes.slice(skip, skip + safePageSize);

    const videos = paged.map((ep, idx) => ({
      ...ep,
      order: skip + idx + 1,
    }));

    return {
      collectionId: collection.id.toString(),
      collectionName: collection.name,
      channelId: collection.channelId.toString(),
      channelName: collection.channel?.name || '-',
      title: `${collection.name} 合集目录`,
      videos,
      page: safePage,
      pageSize: safePageSize,
      total,
      totalPages: Math.max(1, Math.ceil(total / safePageSize)),
    };
  }

  async updateCatalogTitle(
    id: string,
    body: { episodeId?: string; title?: string },
    userId?: string,
    role?: string,
  ) {
    const episodeId = String(body.episodeId || '').trim();
    const title = String(body.title || '').trim();

    if (!episodeId) {
      throw new BadRequestException('episodeId is required');
    }
    if (!title) {
      throw new BadRequestException('title is required');
    }

    const collection = await this.prisma.collection.findFirst({
      where:
        role === 'admin'
          ? { id: BigInt(id) }
          : { id: BigInt(id), createdBy: userId ? BigInt(userId) : undefined },
      select: { id: true, channelId: true, name: true },
    });
    if (!collection) throw new NotFoundException('collection not found');

    const episode = await this.prisma.collectionEpisode.findFirst({
      where: {
        id: BigInt(episodeId),
        collectionId: collection.id,
      },
      select: {
        id: true,
        mediaAssetId: true,
      },
    });

    if (episode) {
      await this.prisma.collectionEpisode.update({
        where: { id: episode.id },
        data: {
          episodeTitle: title,
        },
      });
    } else {
      const taskIdText = episodeId.startsWith('dispatch-') ? episodeId.slice('dispatch-'.length) : '';
      if (!taskIdText || !/^\d+$/.test(taskIdText)) {
        throw new NotFoundException('collection episode not found');
      }

      const dispatchTask = await this.prisma.dispatchTask.findFirst({
        where: {
          id: BigInt(taskIdText),
          channelId: collection.channelId,
          status: 'success',
          telegramMessageLink: { not: null },
        },
        select: {
          mediaAssetId: true,
          telegramMessageLink: true,
          mediaAsset: {
            select: {
              originalName: true,
              sourceMeta: true,
            },
          },
        },
      });

      if (!dispatchTask) {
        throw new NotFoundException('collection episode not found');
      }

      const parsed = parseCollectionMeta(dispatchTask.mediaAsset?.sourceMeta);
      if (!parsed || parsed.collectionName !== collection.name) {
        throw new NotFoundException('collection episode not found');
      }

      const fileNameSnapshot = getFileStem(dispatchTask.mediaAsset?.originalName || '') || `第${parsed.episodeNo}集`;

      await this.prisma.collectionEpisode.upsert({
        where: {
          mediaAssetId: dispatchTask.mediaAssetId,
        },
        update: {
          collectionId: collection.id,
          episodeNo: parsed.episodeNo,
          fileNameSnapshot,
          episodeTitle: title,
        },
        create: {
          collectionId: collection.id,
          mediaAssetId: dispatchTask.mediaAssetId,
          episodeNo: parsed.episodeNo,
          fileNameSnapshot,
          parseStatus: 'ok',
          sortKey: String(parsed.episodeNo).padStart(6, '0'),
          telegramMessageLink: dispatchTask.telegramMessageLink,
          publishedAt: new Date(),
          episodeTitle: title,
        },
      });
    }

    await this.enqueueSearchIndexJob({
      sourceType: 'collection',
      sourceId: collection.id.toString(),
      channelId: collection.channelId.toString(),
    });

    return { ok: true };
  }

  async remove(id: string, userId?: string, role?: string) {
    const existing = await this.prisma.collection.findFirst({
      where:
        role === 'admin'
          ? { id: BigInt(id) }
          : { id: BigInt(id), createdBy: userId ? BigInt(userId) : undefined },
      select: {
        id: true,
        name: true,
        channel: {
          select: { folderPath: true },
        },
      },
    });
    if (!existing) throw new NotFoundException('collection not found');

    try {
      await this.prisma.collection.delete({
        where: { id: BigInt(id) },
      });

      await this.removeCollectionFolderIfExists({
        channelFolderPath: existing.channel.folderPath,
        collectionName: existing.name,
      });

      await this.enqueueSearchIndexJob({
        sourceType: 'collection',
        sourceId: existing.id.toString(),
        action: 'delete',
      });

      return { ok: true };
    } catch (error) {
      throw new InternalServerErrorException(
        `删除合集失败: ${error instanceof Error ? error.message : 'unknown_error'}`,
      );
    }
  }

  private async enqueueSearchIndexJob(payload: {
    sourceType: string;
    sourceId: string;
    channelId?: string;
    action?: 'upsert' | 'delete';
  }) {
    try {
      const queue = getSearchIndexQueue();
      await queue.add('upsert', payload, {
        jobId: `search-index-${payload.sourceType}-${payload.sourceId}`,
        removeOnComplete: true,
        removeOnFail: 200,
      });
    } catch (error) {
      console.error('[collection] 搜索索引入队失败（不阻塞）', error);
    }
  }
}

import { Injectable, InternalServerErrorException, NotFoundException } from '@nestjs/common';
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
    return {
      ...row,
      id: row.id.toString(),
      channelId: row.channelId.toString(),
      channel: row.channel
        ? {
            ...row.channel,
            id: row.channel.id.toString(),
          }
        : undefined,
      _count: row._count,
    };
  }

  async list() {
    const rows = await this.prisma.collection.findMany({
      orderBy: { updatedAt: 'desc' },
      include: {
        channel: { select: { id: true, name: true } },
        _count: { select: { episodes: true } },
      },
    });

    return rows.map((row) => this.toResponse(row));
  }

  async create(dto: SaveCollectionDto) {
    const channel = await this.prisma.channel.findUnique({
      where: { id: BigInt(dto.channelId) },
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
      },
      include: {
        channel: { select: { id: true, name: true } },
        _count: { select: { episodes: true } },
      },
    });

    return this.toResponse(created);
  }

  async update(id: string, dto: Partial<SaveCollectionDto>) {
    const existing = await this.prisma.collection.findUnique({
      where: { id: BigInt(id) },
      select: { id: true, channelId: true, channel: { select: { folderPath: true } } },
    });
    if (!existing) throw new NotFoundException('collection not found');

    if (dto.name || dto.channelId) {
      const channelId = dto.channelId ? BigInt(dto.channelId) : existing.channelId;
      const channel = await this.prisma.channel.findUnique({
        where: { id: channelId },
        select: { id: true, folderPath: true },
      });

      if (channel) {
        const collectionName = (dto.name || '').trim();
        if (collectionName) {
          await this.ensureCollectionFolderExists({
            channelFolderPath: channel.folderPath,
            collectionName,
          });
        }
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

    return this.toResponse(updated);
  }

  async remove(id: string) {
    const existing = await this.prisma.collection.findUnique({
      where: { id: BigInt(id) },
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

      return { ok: true };
    } catch (error) {
      throw new InternalServerErrorException(
        `删除合集失败: ${error instanceof Error ? error.message : 'unknown_error'}`,
      );
    }
  }
}

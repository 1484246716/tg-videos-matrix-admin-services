import {
  ConflictException,
  Injectable,
  InternalServerErrorException,
  NotFoundException,
} from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { PrismaClientKnownRequestError } from '@prisma/client/runtime/library';
import { access, mkdir, rename, rm } from 'node:fs/promises';
import { dirname, normalize, resolve } from 'node:path';
import { PrismaService } from '../prisma/prisma.service';
import { CreateChannelDto } from './dto/create-channel.dto';
import { UpdateChannelDto } from './dto/update-channel.dto';
import { UpdateChannelStatusDto } from './dto/update-channel-status.dto';

@Injectable()
export class ChannelService {
  constructor(private readonly prisma: PrismaService) { }

  private serializeBigInt<T>(value: T): T {
    return JSON.parse(
      JSON.stringify(value, (_key, v) =>
        typeof v === 'bigint' ? v.toString() : v,
      ),
    ) as T;
  }

  private getChannelsRootDir() {
    const raw = (process.env.CHANNELS_ROOT_DIR || './data/channels').trim();

    if (/^\/[a-zA-Z]/.test(raw)) {
      const driveRelative = raw.replace(/^\//, '');
      const workspaceRoot = resolve(process.cwd(), '..', '..');
      return resolve(workspaceRoot, driveRelative);
    }

    return resolve(raw);
  }

  private resolveChannelFolderPath(folderPath: string) {
    const root = this.getChannelsRootDir();
    const normalizedInput = normalize(folderPath.trim().replace(/\\/g, '/'));
    const relativePath = normalizedInput.replace(/^[\\/]+/, '');

    if (!relativePath) {
      throw new ConflictException('目录路径不能为空');
    }

    if (/^[a-zA-Z]:/.test(relativePath)) {
      throw new ConflictException('目录路径不合法，禁止使用盘符绝对路径');
    }

    const absolutePath = resolve(root, relativePath);

    const safeRoot = root.endsWith('\/') ? root : `${root}/`;
    const safeRootWin = root.endsWith('\\') ? root : `${root}\\`;

    if (
      absolutePath !== root &&
      !absolutePath.startsWith(safeRoot) &&
      !absolutePath.startsWith(safeRootWin)
    ) {
      throw new ConflictException('目录路径不合法，禁止越权访问');
    }

    return absolutePath;
  }

  private async ensureFolderCreated(folderPath: string) {
    const target = this.resolveChannelFolderPath(folderPath);
    await mkdir(target, { recursive: true });
  }

  private async moveOrCreateFolder(oldPath: string, newPath: string) {
    const from = this.resolveChannelFolderPath(oldPath);
    const to = this.resolveChannelFolderPath(newPath);

    if (from === to) return;

    await mkdir(dirname(to), { recursive: true });

    try {
      await access(from);
      await rename(from, to);
    } catch {
      await mkdir(to, { recursive: true });
    }
  }

  private async removeFolder(folderPath: string) {
    const target = this.resolveChannelFolderPath(folderPath);
    await rm(target, { recursive: true, force: true });
  }

  async list() {
    const rows = await this.prisma.channel.findMany({
      orderBy: { createdAt: 'desc' },
      include: {
        defaultBot: {
          select: { id: true, name: true, status: true },
        },
        aiModelProfile: {
          select: {
            id: true,
            name: true,
            provider: true,
            model: true,
            isActive: true,
          },
        },
      },
    });

    return this.serializeBigInt(rows);
  }

  async create(dto: CreateChannelDto) {
    try {
      await this.ensureFolderCreated(dto.folderPath);

      const created = await this.prisma.channel.create({
        data: {
          name: dto.name,
          tgChatId: dto.tgChatId,
          tgUsername: dto.tgUsername,
          folderPath: dto.folderPath,
          postIntervalSec: dto.postIntervalSec ?? 120,
          defaultBotId: dto.defaultBotId ? BigInt(dto.defaultBotId) : undefined,
          navEnabled: dto.navEnabled ?? false,
          navIntervalSec: dto.navIntervalSec ?? 604800,
          aiSystemPromptTemplate: dto.aiSystemPromptTemplate,
          navTemplateText: dto.navTemplateText,
          aiReplyMarkup: dto.aiReplyMarkup as Prisma.InputJsonValue,
          navReplyMarkup: dto.navReplyMarkup as Prisma.InputJsonValue,
        },
      });

      return this.serializeBigInt(created);
    } catch (error) {
      if (
        error instanceof PrismaClientKnownRequestError &&
        error.code === 'P2002'
      ) {
        throw new ConflictException('频道 ChatId 已存在，请勿重复创建');
      }

      if (error instanceof ConflictException) {
        throw error;
      }

      throw new InternalServerErrorException('创建频道失败，目录或数据库操作异常');
    }
  }

  async getOne(id: string) {
    const item = await this.prisma.channel.findUnique({
      where: { id: BigInt(id) },
      include: {
        defaultBot: {
          select: { id: true, name: true, status: true },
        },
        relayChannel: {
          select: { id: true, name: true, tgChatId: true, isActive: true },
        },
        aiModelProfile: {
          select: {
            id: true,
            name: true,
            provider: true,
            model: true,
            isActive: true,
          },
        },
        catalogTemplate: {
          select: { id: true, name: true, isActive: true },
        },
      },
    });

    if (!item) {
      throw new NotFoundException('channel not found');
    }

    return this.serializeBigInt(item);
  }

  async update(id: string, dto: UpdateChannelDto) {
    const existing = await this.prisma.channel.findUnique({
      where: { id: BigInt(id) },
      select: { id: true, folderPath: true },
    });

    if (!existing) {
      throw new NotFoundException('channel not found');
    }

    const nextFolderPath = dto.folderPath ?? existing.folderPath;

    const data: Prisma.ChannelUncheckedUpdateInput = {
      name: dto.name,
      tgChatId: dto.tgChatId,
      tgUsername: dto.tgUsername,
      folderPath: dto.folderPath,
      status: dto.status,
      postIntervalSec: dto.postIntervalSec,
      postJitterMinSec: dto.postJitterMinSec,
      postJitterMaxSec: dto.postJitterMaxSec,
      navIntervalSec: dto.navIntervalSec,
      navRecentLimit: dto.navRecentLimit,
      adEnabled: dto.adEnabled,
      adPinEnabled: dto.adPinEnabled,
      alistTargetPath: dto.alistTargetPath,
      autoImportEnabled: dto.autoImportEnabled,
      navEnabled: dto.navEnabled,
      defaultBotId: dto.defaultBotId ? BigInt(dto.defaultBotId) : undefined,
      relayChannelId: dto.relayChannelId ? BigInt(dto.relayChannelId) : undefined,
      aiModelProfileId: dto.aiModelProfileId
        ? BigInt(dto.aiModelProfileId)
        : undefined,
      catalogTemplateId: dto.catalogTemplateId
        ? BigInt(dto.catalogTemplateId)
        : undefined,
      aiSystemPromptTemplate: dto.aiSystemPromptTemplate,
      navTemplateText: dto.navTemplateText,
      aiReplyMarkup: dto.aiReplyMarkup as Prisma.InputJsonValue,
      navReplyMarkup: dto.navReplyMarkup as Prisma.InputJsonValue,
    };

    try {
      await this.moveOrCreateFolder(existing.folderPath, nextFolderPath);

      const updated = await this.prisma.channel.update({
        where: { id: BigInt(id) },
        data,
      });
      return this.serializeBigInt(updated);
    } catch (error) {
      if (
        error instanceof PrismaClientKnownRequestError &&
        error.code === 'P2002'
      ) {
        throw new ConflictException('频道 ChatId 已存在，请使用其他 ChatId');
      }

      if (error instanceof ConflictException) {
        throw error;
      }

      throw new InternalServerErrorException('更新频道失败，目录或数据库操作异常');
    }
  }

  async updateStatus(id: string, dto: UpdateChannelStatusDto) {
    await this.getOne(id);

    const updated = await this.prisma.channel.update({
      where: { id: BigInt(id) },
      data: {
        status: dto.status,
      },
    });

    return this.serializeBigInt(updated);
  }

  async remove(id: string) {
    const existing = await this.prisma.channel.findUnique({
      where: { id: BigInt(id) },
      select: { id: true, folderPath: true },
    });

    if (!existing) {
      throw new NotFoundException('channel not found');
    }

    try {
      const deleted = await this.prisma.channel.delete({
        where: { id: BigInt(id) },
      });

      await this.removeFolder(existing.folderPath);

      return this.serializeBigInt(deleted);
    } catch (error) {
      throw new InternalServerErrorException('删除频道失败，目录或数据库操作异常');
    }
  }
}

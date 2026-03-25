import {
  BadRequestException,
  ConflictException,
  Injectable,
  InternalServerErrorException,
  NotFoundException,
  HttpException,
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

  private parseSafeBigInt(value: string) {
    const raw = value.trim();
    if (!raw) return null;
    if (!/^\d+$/.test(raw)) return null;
    try {
      return BigInt(raw);
    } catch {
      return null;
    }
  }

  private serializeBigInt<T>(value: T): T {
    return JSON.parse(
      JSON.stringify(value, (_key, v) =>
        typeof v === 'bigint' ? v.toString() : v,
      ),
    ) as T;
  }

  private getTelegramBotApiBase() {
    return (
      (process.env.TELEGRAM_BOT_API_BASE || 'https://api.telegram.org').trim() ||
      'https://api.telegram.org'
    ).replace(/\/$/, '');
  }

  private async assertBotIsChannelAdmin(botId: string, tgChatId: string) {
    const bot = await this.prisma.bot.findUnique({
      where: { id: BigInt(botId) },
      select: { id: true, tokenEncrypted: true, name: true },
    });

    if (!bot) {
      throw new BadRequestException('请先选择有效机器人');
    }

    const token = (bot.tokenEncrypted || '').trim();
    if (!token) {
      throw new BadRequestException('机器人 Token 为空，无法校验管理员权限');
    }

    const apiBase = this.getTelegramBotApiBase();
    const chatId = tgChatId.trim();

    const meResp = await fetch(`${apiBase}/bot${token}/getMe`);
    if (!meResp.ok) {
      throw new BadRequestException(`机器人可用性校验失败: HTTP ${meResp.status}`);
    }

    const meData = (await meResp.json()) as {
      ok?: boolean;
      result?: { id?: number };
      description?: string;
    };

    if (!meData.ok || !meData.result?.id) {
      throw new BadRequestException(
        `机器人可用性校验失败: ${meData.description || 'getMe failed'}`,
      );
    }

    const memberResp = await fetch(
      `${apiBase}/bot${token}/getChatMember?chat_id=${encodeURIComponent(chatId)}&user_id=${meData.result.id}`,
    );

    const memberData = (await memberResp.json().catch(() => ({}))) as {
      ok?: boolean;
      result?: { status?: string };
      description?: string;
      error_code?: number;
    };

    if (!memberResp.ok || !memberData.ok) {
      const desc = (memberData.description || '').toLowerCase();

      if (
        desc.includes('chat not found') ||
        desc.includes('user not found') ||
        desc.includes('member list is inaccessible') ||
        desc.includes('bot is not a member')
      ) {
        throw new BadRequestException('请先将机器人加入目标频道并设为管理员后再添加');
      }

      if (desc.includes('need administrator rights') || desc.includes('not enough rights')) {
        throw new BadRequestException('请将机器人设为管理后添加机器人');
      }

      throw new BadRequestException(
        `管理员身份校验失败: ${memberData.description || `HTTP ${memberResp.status}`}`,
      );
    }

    const status = memberData.result?.status;
    if (status !== 'administrator' && status !== 'creator') {
      throw new BadRequestException('请将机器人设为管理后添加机器人');
    }
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
    } catch {
      throw new ConflictException('原目录不存在，禁止自动新建目录，请先确认原目录路径');
    }

    try {
      await rename(from, to);
    } catch {
      throw new ConflictException('目录迁移失败，未执行自动新建，请检查目标路径是否可用');
    }
  }

  private async removeFolder(folderPath: string) {
    const target = this.resolveChannelFolderPath(folderPath);
    await rm(target, { recursive: true, force: true });
  }

  async list(
    userId?: string,
    role?: string,
    filters?: {
      status?: string;
      keyword?: string;
    },
  ) {
    const where: Prisma.ChannelWhereInput =
      role === 'admin'
        ? {}
        : {
            createdBy: userId ? BigInt(userId) : undefined,
          };

    const status = (filters?.status || '').trim();
    if (status && ['active', 'paused', 'archived'].includes(status)) {
      where.status = status as any;
    }

    const keyword = (filters?.keyword || '').trim();
    if (keyword) {
      const idValue = this.parseSafeBigInt(keyword);
      where.OR = [
        { name: { contains: keyword, mode: 'insensitive' } },
        ...(idValue ? [{ id: idValue }] : []),
      ];
    }

    const rows = await this.prisma.channel.findMany({
      where,
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

  async create(dto: CreateChannelDto, userId?: string, role?: string) {
    try {
      if (dto.defaultBotId) {
        await this.assertBotIsChannelAdmin(dto.defaultBotId, dto.tgChatId);
      }

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
          tags: dto.tags ?? [],
          createdBy: role === 'admin' ? null : userId ? BigInt(userId) : null,
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

      if (error instanceof HttpException) {
        throw error;
      }

      throw new InternalServerErrorException('创建频道失败，目录或数据库操作异常');
    }
  }

  async getOne(id: string, userId?: string, role?: string) {
    const item = await this.prisma.channel.findFirst({
      where:
        role === 'admin'
          ? { id: BigInt(id) }
          : {
              id: BigInt(id),
              createdBy: userId ? BigInt(userId) : undefined,
            },
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

  async batchUpdate(
    ids: string[],
    data: {
      postIntervalSec?: number;
      navIntervalSec?: number;
      navEnabled?: boolean;
      defaultBotId?: string | null;
      aiSystemPromptTemplate?: string;
      navTemplateText?: string;
      aiReplyMarkup?: Prisma.InputJsonValue;
      navReplyMarkup?: Prisma.InputJsonValue;
      tags?: string[];
    },
    userId?: string,
    role?: string,
  ) {
    if (!Array.isArray(ids) || ids.length === 0) {
      throw new ConflictException('请选择需要批量更新的频道');
    }

    const validIds = ids.filter((id) => id && id.trim());
    if (validIds.length === 0) {
      throw new ConflictException('请选择需要批量更新的频道');
    }

    if (role !== 'admin') {
      const allowed = await this.prisma.channel.count({
        where: {
          id: { in: validIds.map((id) => BigInt(id)) },
          createdBy: userId ? BigInt(userId) : undefined,
        },
      });
      if (allowed !== validIds.length) {
        throw new ConflictException('存在无权限的频道');
      }
    }

    await this.prisma.channel.updateMany({
      where: { id: { in: validIds.map((id) => BigInt(id)) } },
      data: {
        postIntervalSec: data.postIntervalSec,
        navIntervalSec: data.navIntervalSec,
        navEnabled: data.navEnabled,
        defaultBotId:
          data.defaultBotId === null
            ? null
            : data.defaultBotId
              ? BigInt(data.defaultBotId)
              : undefined,
        aiSystemPromptTemplate: data.aiSystemPromptTemplate,
        navTemplateText: data.navTemplateText,
        aiReplyMarkup: data.aiReplyMarkup,
        navReplyMarkup: data.navReplyMarkup,
        tags: data.tags,
      },
    });

    return { updated: validIds.length };
  }

  async getCatalogPreview(id: string, userId?: string, role?: string) {
    const channel = await this.prisma.channel.findFirst({
      where:
        role === 'admin'
          ? { id: BigInt(id) }
          : { id: BigInt(id), createdBy: userId ? BigInt(userId) : undefined },
      select: {
        id: true,
        name: true,
        tgChatId: true,
        navTemplateText: true,
      },
    });

    if (!channel) throw new NotFoundException('channel not found');

    const dispatchTasks = await this.prisma.dispatchTask.findMany({
      where: {
        channelId: channel.id,
        status: 'success',
        telegramMessageLink: { not: null },
      },
      orderBy: { finishedAt: 'desc' },
      take: 60,
      select: {
        id: true,
        mediaAssetId: true,
        caption: true,
        telegramMessageLink: true,
        mediaAsset: {
          select: {
            sourceMeta: true,
          },
        },
      },
    });

    const videos = [...dispatchTasks].reverse().map((t, idx) => {
      const caption = (t.caption || '').trim();
      const titleMatch = caption.match(/(?:^|\n)\s*📺?\s*片名\s*[：:]\s*(.+)/);
      const actorMatch = caption.match(/(?:^|\n)\s*(?:👥\s*)?主演\s*[：:]\s*(.+)/);

      const parts = caption
        .split('\n')
        .map((l) => l.trim())
        .filter(Boolean);

      let shortTitle = '未命名视频';
      if (titleMatch?.[1]?.trim()) {
        const rawTitle = titleMatch[1].trim();
        const wrapped = rawTitle.match(/《[^》]+》/);
        const displayTitle = wrapped ? wrapped[0] : `《${rawTitle.replace(/^《|》$/g, '').trim()}》`;
        shortTitle = actorMatch?.[1]?.trim()
          ? `📺片名：${displayTitle} 👥主演: ${actorMatch[1].trim()}`
          : `📺片名：${displayTitle}`;
      } else if (parts.length >= 2) {
        shortTitle = `${parts[0]} ${parts[1]}`;
      } else if (parts.length === 1) {
        shortTitle = parts[0];
      }

      const sourceMeta =
        t.mediaAsset?.sourceMeta && typeof t.mediaAsset.sourceMeta === 'object'
          ? (t.mediaAsset.sourceMeta as Record<string, unknown>)
          : {};
      const customCatalogTitle =
        typeof sourceMeta.catalogCustomTitle === 'string' ? sourceMeta.catalogCustomTitle.trim() : '';

      const link = t.telegramMessageLink || '';
      return {
        id: t.id.toString(),
        mediaAssetId: t.mediaAssetId.toString(),
        title: customCatalogTitle || shortTitle,
        link,
        readonlyLink: link,
        order: idx + 1,
      };
    });

    const titleLineRaw = (channel.navTemplateText || '').split('\n').find((line) => line.trim()) || '';
    const defaultTitle = `${channel.name} 知识库/资源大全`;
    const title = titleLineRaw
      .replace(/{{channel_name}}/g, channel.name)
      .trim() || defaultTitle;

    return this.serializeBigInt({
      channelId: channel.id,
      channelName: channel.name,
      tgChatId: channel.tgChatId,
      title,
      videos,
    });
  }

  async updateCatalogTitle(
    id: string,
    payload: { mediaAssetId?: string; title?: string },
    userId?: string,
    role?: string,
  ) {
    const channel = await this.prisma.channel.findFirst({
      where:
        role === 'admin'
          ? { id: BigInt(id) }
          : { id: BigInt(id), createdBy: userId ? BigInt(userId) : undefined },
      select: {
        id: true,
      },
    });

    if (!channel) throw new NotFoundException('channel not found');

    const mediaAssetIdRaw = (payload.mediaAssetId || '').trim();
    const nextTitle = (payload.title || '').trim();

    if (!mediaAssetIdRaw) throw new BadRequestException('mediaAssetId 不能为空');
    if (!nextTitle) throw new BadRequestException('目录标题不能为空');

    const mediaAssetId = BigInt(mediaAssetIdRaw);

    const mediaAsset = await this.prisma.mediaAsset.findFirst({
      where: {
        id: mediaAssetId,
        channelId: channel.id,
      },
      select: {
        id: true,
        sourceMeta: true,
      },
    });

    if (!mediaAsset) throw new NotFoundException('mediaAsset not found');

    const sourceMeta =
      mediaAsset.sourceMeta && typeof mediaAsset.sourceMeta === 'object'
        ? (mediaAsset.sourceMeta as Record<string, unknown>)
        : {};

    const updated = await this.prisma.mediaAsset.update({
      where: { id: mediaAsset.id },
      data: {
        sourceMeta: {
          ...sourceMeta,
          catalogCustomTitle: nextTitle,
          catalogCustomTitleUpdatedAt: new Date().toISOString(),
        },
      },
      select: {
        id: true,
      },
    });

    return this.serializeBigInt({
      id: updated.id,
      mediaAssetId: updated.id,
      title: nextTitle,
    });
  }

  async update(id: string, dto: UpdateChannelDto, userId?: string, role?: string) {
    const existing = await this.prisma.channel.findFirst({
      where:
        role === 'admin'
          ? { id: BigInt(id) }
          : { id: BigInt(id), createdBy: userId ? BigInt(userId) : undefined },
      select: { id: true, folderPath: true, tgChatId: true, defaultBotId: true },
    });

    if (!existing) {
      throw new NotFoundException('channel not found');
    }

    const nextFolderPath = dto.folderPath ?? existing.folderPath;
    const nextTgChatId = dto.tgChatId ?? existing.tgChatId;

    if (dto.defaultBotId) {
      const shouldCheckBotAdmin =
        dto.defaultBotId !== (existing.defaultBotId ? existing.defaultBotId.toString() : undefined) ||
        nextTgChatId !== existing.tgChatId;

      if (shouldCheckBotAdmin) {
        await this.assertBotIsChannelAdmin(dto.defaultBotId, nextTgChatId);
      }
    }

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
      tags: dto.tags,
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

  async updateStatus(
    id: string,
    dto: UpdateChannelStatusDto,
    userId?: string,
    role?: string,
  ) {
    await this.getOne(id, userId, role);

    const updated = await this.prisma.channel.update({
      where: { id: BigInt(id) },
      data: {
        status: dto.status,
      },
    });

    return this.serializeBigInt(updated);
  }

  async remove(id: string, userId?: string, role?: string) {
    const channelId = BigInt(id);

    const existing = await this.prisma.channel.findFirst({
      where:
        role === 'admin'
          ? { id: channelId }
          : { id: channelId, createdBy: userId ? BigInt(userId) : undefined },
      select: { id: true, folderPath: true },
    });

    if (!existing) {
      throw new NotFoundException('channel not found');
    }

    try {
      const deleted = await this.prisma.$transaction(async (tx) => {
        await tx.riskEvent.deleteMany({
          where: { channelId },
        });

        await tx.dispatchTaskLog.deleteMany({
          where: {
            dispatchTask: {
              channelId,
            },
          },
        });

        await tx.dispatchTask.deleteMany({
          where: { channelId },
        });

        await tx.mediaAsset.deleteMany({
          where: { channelId },
        });

        await tx.catalogTask.deleteMany({
          where: { channelId },
        });

        await tx.catalogHistory.deleteMany({
          where: { channelId },
        });

        return tx.channel.delete({
          where: { id: channelId },
        });
      });

      await this.removeFolder(existing.folderPath);

      return this.serializeBigInt(deleted);
    } catch (error) {
      if (
        error instanceof PrismaClientKnownRequestError &&
        error.code === 'P2003'
      ) {
        throw new ConflictException('频道仍有关联数据，请先清理后再删除');
      }

      throw new InternalServerErrorException('删除频道失败，目录或数据库操作异常');
    }
  }
}

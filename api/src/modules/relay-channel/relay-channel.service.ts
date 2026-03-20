import {
  BadRequestException,
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

  private getTelegramBotApiBase() {
    return (
      (process.env.TELEGRAM_BOT_API_BASE || 'https://api.telegram.org').trim() ||
      'https://api.telegram.org'
    ).replace(/\/$/, '');
  }

  private async assertBotIsRelayChannelAdmin(botId: number, tgChatId: string) {
    const bot = await this.prisma.bot.findUnique({
      where: { id: BigInt(botId) },
      select: { id: true, tokenEncrypted: true },
    });

    if (!bot) {
      throw new BadRequestException('请先选择有效机器人');
    }

    const token = (bot.tokenEncrypted || '').trim();
    if (!token) {
      throw new BadRequestException('机器人 Token 为空，无法校验管理员权限');
    }

    const apiBase = this.getTelegramBotApiBase();
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
      `${apiBase}/bot${token}/getChatMember?chat_id=${encodeURIComponent(tgChatId)}&user_id=${meData.result.id}`,
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
      await this.assertBotIsRelayChannelAdmin(dto.botId, dto.tgChatId);

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

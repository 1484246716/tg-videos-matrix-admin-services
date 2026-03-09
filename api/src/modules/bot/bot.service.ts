import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { CreateBotDto } from './dto/create-bot.dto';
import { UpdateBotDto } from './dto/update-bot.dto';

@Injectable()
export class BotService {
  constructor(private readonly prisma: PrismaService) {}

  async list() {
    const rows = await this.prisma.bot.findMany({
      orderBy: { createdAt: 'desc' },
      select: {
        id: true,
        name: true,
        username: true,
        telegramBotId: true,
        tokenMasked: true,
        status: true,
        rateLimitPerMin: true,
        lastHealthCheckAt: true,
        createdAt: true,
        updatedAt: true,
      },
    });

    return rows.map((item) => ({
      ...item,
      id: item.id.toString(),
      telegramBotId: item.telegramBotId ? item.telegramBotId.toString() : null,
    }));
  }

  async create(dto: CreateBotDto) {
    const tokenMasked =
      dto.tokenMasked && dto.tokenMasked.trim().length > 0
        ? dto.tokenMasked
        : this.maskToken(dto.tokenEncrypted);

    const created = await this.prisma.bot.create({
      data: {
        name: dto.name,
        tokenEncrypted: dto.tokenEncrypted,
        tokenMasked,
        telegramBotId: dto.telegramBotId,
        username: dto.username ?? dto.name,
        rateLimitPerMin: dto.rateLimitPerMin ?? 8,
      },
      select: {
        id: true,
        name: true,
        username: true,
        telegramBotId: true,
        tokenMasked: true,
        status: true,
        rateLimitPerMin: true,
        createdAt: true,
        updatedAt: true,
      },
    });

    return {
      ...created,
      id: created.id.toString(),
      telegramBotId: created.telegramBotId ? created.telegramBotId.toString() : null,
    };
  }

  async update(id: string, dto: UpdateBotDto) {
    const botId = BigInt(id);

    const existing = await this.prisma.bot.findUnique({
      where: { id: botId },
      select: { id: true },
    });

    if (!existing) {
      throw new NotFoundException('bot not found');
    }

    const tokenEncrypted = dto.tokenEncrypted?.trim();

    const updated = await this.prisma.bot.update({
      where: { id: botId },
      data: {
        name: dto.name,
        tokenEncrypted: tokenEncrypted,
        tokenMasked:
          dto.tokenMasked && dto.tokenMasked.trim().length > 0
            ? dto.tokenMasked
            : tokenEncrypted
              ? this.maskToken(tokenEncrypted)
              : undefined,
        telegramBotId: dto.telegramBotId,
        username: dto.username ?? dto.name,
        rateLimitPerMin: dto.rateLimitPerMin,
      },
      select: {
        id: true,
        name: true,
        username: true,
        telegramBotId: true,
        tokenMasked: true,
        status: true,
        rateLimitPerMin: true,
        createdAt: true,
        updatedAt: true,
      },
    });

    return {
      ...updated,
      id: updated.id.toString(),
      telegramBotId: updated.telegramBotId ? updated.telegramBotId.toString() : null,
    };
  }

  async remove(id: string) {
    const botId = BigInt(id);

    const usedByChannels = await this.prisma.channel.count({
      where: { defaultBotId: botId },
    });

    if (usedByChannels > 0) {
      throw new BadRequestException('bot is referenced by channels, unbind first');
    }

    await this.prisma.bot.delete({ where: { id: botId } });

    return { ok: true };
  }

  private maskToken(raw: string) {
    const token = raw.trim();
    if (!token) return '';
    if (token.length <= 10) return `${token.slice(0, 2)}***`;
    return `${token.slice(0, 6)}***${token.slice(-4)}`;
  }
}

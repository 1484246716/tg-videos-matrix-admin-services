import {
  BadRequestException,
  Injectable,
  InternalServerErrorException,
  NotFoundException,
} from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { CreateMessageTemplateDto } from './dto/create-message-template.dto';
import { UpdateMessageTemplateDto } from './dto/update-message-template.dto';

@Injectable()
export class MessageTemplateService {
  constructor(private readonly prisma: PrismaService) {}

  private get model() {
    const model = this.prisma.messageTemplate;
    if (!model) {
      throw new InternalServerErrorException(
        'Prisma messageTemplate model is unavailable. Please run prisma generate and restart API.',
      );
    }
    return model;
  }

  private serializeBigInt<T>(value: T): T {
    return JSON.parse(
      JSON.stringify(value, (_key, v) => (typeof v === 'bigint' ? v.toString() : v)),
    ) as T;
  }

  async list(params: { isActive?: string; limit?: number }) {
    const rows = await this.model.findMany({
      where: {
        isActive:
          params.isActive === undefined ? undefined : params.isActive === 'true',
      },
      orderBy: { updatedAt: 'desc' },
      take: params.limit ?? 200,
    });
    return this.serializeBigInt(rows);
  }

  async getOne(id: string) {
    const row = await this.model.findUnique({ where: { id: BigInt(id) } });
    if (!row) throw new NotFoundException('message template not found');
    return row;
  }

  async create(dto: CreateMessageTemplateDto, createdBy?: bigint) {
    if (!dto.name.trim()) {
      throw new BadRequestException('name is required');
    }
    const created = await this.model.create({
      data: {
        name: dto.name.trim(),
        format: dto.format,
        content: dto.content,
        imageUrl: dto.imageUrl,
        buttons: dto.buttons as object | undefined,
        variables: dto.variables ?? [],
        isActive: dto.isActive ?? true,
        createdBy,
      },
    });
    return this.serializeBigInt(created);
  }

  async update(id: string, dto: UpdateMessageTemplateDto) {
    await this.getOne(id);
    const updated = await this.model.update({
      where: { id: BigInt(id) },
      data: {
        name: dto.name?.trim(),
        format: dto.format,
        content: dto.content,
        imageUrl: dto.imageUrl,
        buttons: dto.buttons as object | undefined,
        variables: dto.variables,
        isActive: dto.isActive,
      },
    });
    return this.serializeBigInt(updated);
  }

  async remove(id: string) {
    await this.getOne(id);
    // soft delete for safety
    const updated = await this.model.update({
      where: { id: BigInt(id) },
      data: { isActive: false },
    });
    return this.serializeBigInt(updated);
  }
}


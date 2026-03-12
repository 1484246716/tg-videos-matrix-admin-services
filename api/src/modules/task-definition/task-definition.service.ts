import {
  BadRequestException,
  Injectable,
  InternalServerErrorException,
  NotFoundException,
} from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { CreateTaskDefinitionDto } from './dto/create-task-definition.dto';
import { UpdateTaskDefinitionDto } from './dto/update-task-definition.dto';

const RUN_INTERVAL_OPTIONS = new Set([30, 120, 1800, 3600]);

@Injectable()
export class TaskDefinitionService {
  constructor(private readonly prisma: PrismaService) {}

  private get taskDefinitionModel() {
    const model = this.prisma.taskDefinition;
    if (!model) {
      throw new InternalServerErrorException(
        'Prisma taskDefinition model is unavailable. Please run prisma generate and restart API.',
      );
    }

    return model;
  }

  private serializeBigInt<T>(value: T): T {
    return JSON.parse(
      JSON.stringify(value, (_key, v) =>
        typeof v === 'bigint' ? v.toString() : v,
      ),
    ) as T;
  }

  private validateRunIntervalSec(runIntervalSec: number | undefined) {
    if (runIntervalSec === undefined) return;

    if (!RUN_INTERVAL_OPTIONS.has(runIntervalSec)) {
      throw new BadRequestException(
        'runIntervalSec must be one of: 30, 120, 1800, 3600',
      );
    }
  }

  async list(params: {
    taskType?: 'relay_upload' | 'dispatch_send' | 'catalog_publish' | 'mass_message';
    isEnabled?: string;
    limit?: number;
  }) {
    const rows = await this.taskDefinitionModel.findMany({
      where: {
        taskType: params.taskType,
        isEnabled:
          params.isEnabled === undefined
            ? undefined
            : params.isEnabled === 'true',
      },
      orderBy: { createdAt: 'desc' },
      take: params.limit ?? 100,
      include: {
        relayChannel: { select: { id: true, name: true } },
        catalogTemplate: { select: { id: true, name: true } },
      },
    });

    return this.serializeBigInt(rows);
  }

  async create(dto: CreateTaskDefinitionDto) {
    this.validateRunIntervalSec(dto.runIntervalSec);

    const now = new Date();
    const isEnabled = dto.isEnabled ?? true;

    const created = await this.taskDefinitionModel.create({
      data: {
        name: dto.name,
        taskType: dto.taskType,
        isEnabled,
        scheduleCron: dto.scheduleCron,
        relayChannelId: dto.relayChannelId
          ? BigInt(dto.relayChannelId)
          : undefined,
        catalogTemplateId: dto.catalogTemplateId
          ? BigInt(dto.catalogTemplateId)
          : undefined,
        priority: dto.priority ?? 100,
        maxRetries: dto.maxRetries ?? 6,
        runIntervalSec: dto.runIntervalSec ?? 1800,
        nextRunAt: isEnabled ? now : null,
        payload: dto.payload as object | undefined,
      },
      include: {
        relayChannel: { select: { id: true, name: true } },
        catalogTemplate: { select: { id: true, name: true } },
      },
    });

    return this.serializeBigInt(created);
  }

  async update(id: string, dto: UpdateTaskDefinitionDto) {
    const existing = await this.getOne(id);
    this.validateRunIntervalSec(dto.runIntervalSec);

    const nextEnabled = dto.isEnabled ?? existing.isEnabled;
    const shouldResetNextRunAt = dto.isEnabled === true && !existing.isEnabled;

    const updated = await this.taskDefinitionModel.update({
      where: { id: BigInt(id) },
      data: {
        name: dto.name,
        taskType: dto.taskType,
        isEnabled: dto.isEnabled,
        scheduleCron: dto.scheduleCron,
        relayChannelId: dto.relayChannelId ? BigInt(dto.relayChannelId) : undefined,
        catalogTemplateId: dto.catalogTemplateId
          ? BigInt(dto.catalogTemplateId)
          : undefined,
        priority: dto.priority,
        maxRetries: dto.maxRetries,
        runIntervalSec: dto.runIntervalSec,
        nextRunAt: shouldResetNextRunAt
          ? new Date()
          : dto.isEnabled === false
            ? null
            : nextEnabled && existing.nextRunAt === null
              ? new Date()
              : undefined,
        payload: dto.payload as object | undefined,
      },
      include: {
        relayChannel: { select: { id: true, name: true } },
        catalogTemplate: { select: { id: true, name: true } },
      },
    });

    return this.serializeBigInt(updated);
  }

  async toggle(id: string, isEnabled: boolean) {
    const existing = await this.getOne(id);

    const updated = await this.taskDefinitionModel.update({
      where: { id: BigInt(id) },
      data: {
        isEnabled,
        nextRunAt: isEnabled
          ? existing.nextRunAt ?? new Date()
          : null,
      },
      select: {
        id: true,
        name: true,
        taskType: true,
        isEnabled: true,
        runIntervalSec: true,
        nextRunAt: true,
        updatedAt: true,
      },
    });

    return this.serializeBigInt(updated);
  }

  async remove(id: string) {
    await this.getOne(id);
    const deleted = await this.taskDefinitionModel.delete({ where: { id: BigInt(id) } });
    return this.serializeBigInt(deleted);
  }

  async getOne(id: string) {
    const item = await this.taskDefinitionModel.findUnique({
      where: { id: BigInt(id) },
      include: {
        relayChannel: { select: { id: true, name: true } },
        catalogTemplate: { select: { id: true, name: true } },
      },
    });

    if (!item) {
      throw new NotFoundException('task definition not found');
    }

    return item;
  }
}

import { ConflictException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaClientKnownRequestError } from '@prisma/client/runtime/library';
import { PrismaService } from '../prisma/prisma.service';
import { CreateAiModelDto } from './dto/create-ai-model.dto';
import { UpdateAiModelDto } from './dto/update-ai-model.dto';

@Injectable()
export class AiModelService {
  constructor(private readonly prisma: PrismaService) {}

  private serializeBigInt<T>(value: T): T {
    return JSON.parse(
      JSON.stringify(value, (_key, v) =>
        typeof v === 'bigint' ? v.toString() : v,
      ),
    ) as T;
  }

  async list() {
    const rows = await this.prisma.aiModelProfile.findMany({
      orderBy: { createdAt: 'desc' },
      select: {
        id: true,
        name: true,
        provider: true,
        model: true,
        endpointUrl: true,
        temperature: true,
        topP: true,
        maxTokens: true,
        timeoutMs: true,
        isActive: true,
        createdAt: true,
        updatedAt: true,
      },
    });

    return this.serializeBigInt(rows);
  }

  async create(dto: CreateAiModelDto) {
    try {
      const created = await this.prisma.aiModelProfile.create({
        data: {
          name: dto.name,
          provider: dto.provider,
          model: dto.model,
          apiKeyEncrypted: dto.apiKeyEncrypted,
          endpointUrl: dto.endpointUrl,
          systemPrompt: dto.systemPrompt,
          captionPromptTemplate: dto.captionPromptTemplate,
          temperature: dto.temperature,
          topP: dto.topP,
          maxTokens: dto.maxTokens,
          timeoutMs: dto.timeoutMs ?? 20000,
          isActive: dto.isActive ?? true,
        },
        select: {
          id: true,
          name: true,
          provider: true,
          model: true,
          endpointUrl: true,
          temperature: true,
          topP: true,
          maxTokens: true,
          timeoutMs: true,
          isActive: true,
          createdAt: true,
          updatedAt: true,
        },
      });

      return this.serializeBigInt(created);
    } catch (error) {
      if (
        error instanceof PrismaClientKnownRequestError &&
        error.code === 'P2002'
      ) {
        throw new ConflictException('模型名称已存在，请使用其他名称');
      }
      throw error;
    }
  }

  async getOne(id: string) {
    const item = await this.prisma.aiModelProfile.findUnique({
      where: { id: BigInt(id) },
      select: {
        id: true,
        name: true,
        provider: true,
        model: true,
        apiKeyEncrypted: true,
        endpointUrl: true,
        systemPrompt: true,
        captionPromptTemplate: true,
        temperature: true,
        topP: true,
        maxTokens: true,
        timeoutMs: true,
        isActive: true,
        createdAt: true,
        updatedAt: true,
      },
    });

    if (!item) {
      throw new NotFoundException('ai model profile not found');
    }

    return this.serializeBigInt(item);
  }

  async update(id: string, dto: UpdateAiModelDto) {
    await this.getOne(id);

    try {
      const updated = await this.prisma.aiModelProfile.update({
        where: { id: BigInt(id) },
        data: {
          name: dto.name,
          provider: dto.provider,
          model: dto.model,
          apiKeyEncrypted: dto.apiKeyEncrypted,
          endpointUrl: dto.endpointUrl,
          systemPrompt: dto.systemPrompt,
          captionPromptTemplate: dto.captionPromptTemplate,
          temperature: dto.temperature,
          topP: dto.topP,
          maxTokens: dto.maxTokens,
          timeoutMs: dto.timeoutMs,
          isActive: dto.isActive,
        },
        select: {
          id: true,
          name: true,
          provider: true,
          model: true,
          endpointUrl: true,
          temperature: true,
          topP: true,
          maxTokens: true,
          timeoutMs: true,
          isActive: true,
          createdAt: true,
          updatedAt: true,
        },
      });

      return this.serializeBigInt(updated);
    } catch (error) {
      if (
        error instanceof PrismaClientKnownRequestError &&
        error.code === 'P2002'
      ) {
        throw new ConflictException('模型名称已存在，请使用其他名称');
      }
      throw error;
    }
  }

  async remove(id: string) {
    await this.getOne(id);

    const deleted = await this.prisma.aiModelProfile.delete({
      where: { id: BigInt(id) },
      select: {
        id: true,
        name: true,
        provider: true,
        model: true,
        endpointUrl: true,
        temperature: true,
        topP: true,
        maxTokens: true,
        timeoutMs: true,
        isActive: true,
        createdAt: true,
        updatedAt: true,
      },
    });

    return this.serializeBigInt(deleted);
  }
}

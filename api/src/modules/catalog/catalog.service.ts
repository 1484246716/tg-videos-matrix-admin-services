import { Injectable, NotFoundException } from '@nestjs/common';
import { CatalogTaskStatus } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { CreateCatalogTemplateDto } from './dto/create-catalog-template.dto';
import { UpdateCatalogTemplateDto } from './dto/update-catalog-template.dto';
import { RenderCatalogPreviewDto } from './dto/render-catalog-preview.dto';
import { PublishCatalogDto } from './dto/publish-catalog.dto';

@Injectable()
export class CatalogService {
  constructor(private readonly prisma: PrismaService) {}

  async listTemplates() {
    return this.prisma.catalogTemplate.findMany({
      orderBy: { createdAt: 'desc' },
    });
  }

  async getTemplate(id: string) {
    const item = await this.prisma.catalogTemplate.findUnique({
      where: { id: BigInt(id) },
    });

    if (!item) throw new NotFoundException('catalogTemplate not found');
    return item;
  }

  async createTemplate(dto: CreateCatalogTemplateDto) {
    return this.prisma.catalogTemplate.create({
      data: {
        name: dto.name,
        bodyTemplate: dto.bodyTemplate,
        isActive: dto.isActive ?? true,
        recentLimit: dto.recentLimit ?? 60,
      },
    });
  }

  async updateTemplate(id: string, dto: UpdateCatalogTemplateDto) {
    await this.getTemplate(id);
    return this.prisma.catalogTemplate.update({
      where: { id: BigInt(id) },
      data: {
        name: dto.name,
        bodyTemplate: dto.bodyTemplate,
        isActive: dto.isActive,
        recentLimit: dto.recentLimit,
      },
    });
  }

  async deleteTemplate(id: string) {
    await this.getTemplate(id);
    return this.prisma.catalogTemplate.delete({
      where: { id: BigInt(id) },
    });
  }

  async renderPreview(dto: RenderCatalogPreviewDto) {
    const channel = await this.prisma.channel.findUnique({
      where: { id: BigInt(dto.channelId) },
      select: { id: true, name: true, tgUsername: true },
    });

    if (!channel) throw new NotFoundException('channel not found');

    const template = await this.getTemplate(dto.catalogTemplateId);

    const recentLimit = dto.recentLimitOverride ?? template.recentLimit;

    const mediaAssets = await this.prisma.mediaAsset.findMany({
      where: {
        channelId: channel.id,
        status: { in: ['relay_uploaded'] },
      },
      orderBy: { createdAt: 'desc' },
      take: recentLimit,
      select: {
        id: true,
        originalName: true,
        createdAt: true,
      },
    });

    const items = mediaAssets
      .map(
        (item, idx) =>
          `${idx + 1}. ${item.originalName} (${new Date(item.createdAt).toLocaleDateString()})`,
      )
      .join('\n');

    const content = template.bodyTemplate
      .replaceAll('{{channelName}}', channel.name)
      .replaceAll('{{channelUsername}}', channel.tgUsername || '-')
      .replaceAll('{{count}}', String(mediaAssets.length))
      .replaceAll('{{items}}', items || '暂无可展示内容');

    return {
      channelId: channel.id.toString(),
      catalogTemplateId: template.id.toString(),
      recentLimit,
      generatedAt: new Date().toISOString(),
      itemCount: mediaAssets.length,
      content,
    };
  }

  async publish(dto: PublishCatalogDto) {
    const preview = await this.renderPreview({
      channelId: dto.channelId,
      catalogTemplateId: dto.catalogTemplateId,
    });

    const channelId = BigInt(dto.channelId);
    const catalogTemplateId = BigInt(dto.catalogTemplateId);

    const [task] = await this.prisma.$transaction([
      this.prisma.catalogTask.create({
        data: {
          channelId,
          catalogTemplateId,
          status: CatalogTaskStatus.pending,
          plannedAt: new Date(),
          contentPreview: preview.content,
          pinAfterPublish: dto.pinAfterPublish ?? false,
        },
      }),
      this.prisma.catalogHistory.create({
        data: {
          channelId,
          catalogTemplateId,
          content: preview.content,
          renderedCount: preview.itemCount,
          publishedAt: new Date(),
        },
      }),
    ]);

    return task;
  }

  async listTasks(channelId?: string, status?: CatalogTaskStatus, limit?: number) {
    return this.prisma.catalogTask.findMany({
      where: {
        channelId: channelId ? BigInt(channelId) : undefined,
        status,
      },
      orderBy: { createdAt: 'desc' },
      take: limit ?? 100,
      include: {
        channel: {
          select: {
            id: true,
            name: true,
            tgChatId: true,
          },
        },
        catalogTemplate: {
          select: {
            id: true,
            name: true,
          },
        },
      },
    });
  }

  async listHistories(params: {
    channelId?: string;
    catalogTemplateId?: string;
    limit?: number;
  }) {
    return this.prisma.catalogHistory.findMany({
      where: {
        channelId: params.channelId ? BigInt(params.channelId) : undefined,
        catalogTemplateId: params.catalogTemplateId
          ? BigInt(params.catalogTemplateId)
          : undefined,
      },
      orderBy: { publishedAt: 'desc' },
      take: params.limit ?? 100,
      include: {
        channel: {
          select: {
            id: true,
            name: true,
          },
        },
        catalogTemplate: {
          select: {
            id: true,
            name: true,
          },
        },
      },
    });
  }
}

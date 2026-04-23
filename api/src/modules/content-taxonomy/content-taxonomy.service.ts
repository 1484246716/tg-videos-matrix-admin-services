import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

type TaxonomyCategoryView = {
  id: string;
  name: string;
  slug: string;
  sort: number;
  status: string;
  level1Id: string;
  level1Name: string;
  level1Slug: string;
};

type TaxonomyTagView = {
  id: string;
  name: string;
  slug: string;
  sort: number;
  status: string;
  scope: string;
};

@Injectable()
export class ContentTaxonomyService {
  constructor(private readonly prisma: PrismaService) {}

  async listLevel1(params?: { status?: string }) {
    const rows = await this.prisma.categoryLevel1.findMany({
      where: params?.status ? { status: params.status } : undefined,
      orderBy: [{ sort: 'asc' }, { name: 'asc' }],
      include: {
        _count: {
          select: {
            level2List: true,
          },
        },
      },
    });

    return rows.map((row) => ({
      id: row.id.toString(),
      name: row.name,
      slug: row.slug,
      sort: row.sort,
      status: row.status,
      level2Count: row._count.level2List,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
    }));
  }

  async listLevel2(params?: { status?: string; level1Id?: string }) {
    const rows = await this.prisma.categoryLevel2.findMany({
      where: {
        status: params?.status || undefined,
        level1Id: params?.level1Id ? BigInt(params.level1Id) : undefined,
      },
      orderBy: [{ sort: 'asc' }, { name: 'asc' }],
      include: {
        level1: true,
      },
    });

    return rows.map((row) => this.serializeCategory(row));
  }

  async listTags(params?: { status?: string; scope?: string }) {
    const rows = await this.prisma.contentTag.findMany({
      where: {
        status: params?.status || undefined,
        scope: params?.scope || undefined,
      },
      orderBy: [{ sort: 'asc' }, { name: 'asc' }],
    });

    return rows.map((row) => this.serializeTag(row));
  }

  async createLevel1(dto: { name: string; slug: string; sort?: number; status?: string }) {
    const created = await this.prisma.categoryLevel1.create({
      data: {
        name: dto.name.trim(),
        slug: dto.slug.trim(),
        sort: dto.sort ?? 0,
        status: dto.status ?? 'active',
      },
    });

    return {
      id: created.id.toString(),
      name: created.name,
      slug: created.slug,
      sort: created.sort,
      status: created.status,
      createdAt: created.createdAt,
      updatedAt: created.updatedAt,
    };
  }

  async updateLevel1(id: string, dto: Partial<{ name: string; slug: string; sort: number; status: string }>) {
    const updated = await this.prisma.categoryLevel1.update({
      where: { id: BigInt(id) },
      data: {
        name: dto.name?.trim(),
        slug: dto.slug?.trim(),
        sort: dto.sort,
        status: dto.status,
      },
    });

    return {
      id: updated.id.toString(),
      name: updated.name,
      slug: updated.slug,
      sort: updated.sort,
      status: updated.status,
      createdAt: updated.createdAt,
      updatedAt: updated.updatedAt,
    };
  }

  async createLevel2(dto: { level1Id: string; name: string; slug: string; sort?: number; status?: string }) {
    await this.assertLevel1Exists(dto.level1Id);

    const created = await this.prisma.categoryLevel2.create({
      data: {
        level1Id: BigInt(dto.level1Id),
        name: dto.name.trim(),
        slug: dto.slug.trim(),
        sort: dto.sort ?? 0,
        status: dto.status ?? 'active',
      },
      include: {
        level1: true,
      },
    });

    return this.serializeCategory(created);
  }

  async updateLevel2(
    id: string,
    dto: Partial<{ level1Id: string; name: string; slug: string; sort: number; status: string }>,
  ) {
    if (dto.level1Id) {
      await this.assertLevel1Exists(dto.level1Id);
    }

    const updated = await this.prisma.categoryLevel2.update({
      where: { id: BigInt(id) },
      data: {
        level1Id: dto.level1Id ? BigInt(dto.level1Id) : undefined,
        name: dto.name?.trim(),
        slug: dto.slug?.trim(),
        sort: dto.sort,
        status: dto.status,
      },
      include: {
        level1: true,
      },
    });

    return this.serializeCategory(updated);
  }

  async createTag(dto: { name: string; slug: string; sort?: number; status?: string; scope?: string }) {
    const created = await this.prisma.contentTag.create({
      data: {
        name: dto.name.trim(),
        slug: dto.slug.trim(),
        sort: dto.sort ?? 0,
        status: dto.status ?? 'active',
        scope: dto.scope ?? 'adult_18',
      },
    });

    return this.serializeTag(created);
  }

  async updateTag(
    id: string,
    dto: Partial<{ name: string; slug: string; sort: number; status: string; scope: string }>,
  ) {
    const updated = await this.prisma.contentTag.update({
      where: { id: BigInt(id) },
      data: {
        name: dto.name?.trim(),
        slug: dto.slug?.trim(),
        sort: dto.sort,
        status: dto.status,
        scope: dto.scope,
      },
    });

    return this.serializeTag(updated);
  }

  async replaceMediaAssetTaxonomy(mediaAssetIdRaw: string | bigint, payload: { level2Ids?: string[]; tagIds?: string[] }) {
    const mediaAssetId = typeof mediaAssetIdRaw === 'bigint' ? mediaAssetIdRaw : BigInt(mediaAssetIdRaw);
    const level2Ids = this.normalizeIds(payload.level2Ids);
    const tagIds = this.normalizeIds(payload.tagIds);

    await this.assertLevel2IdsExist(level2Ids);
    await this.assertTagIdsExist(tagIds);

    await this.prisma.$transaction(async (tx) => {
      await tx.mediaAssetCategory.deleteMany({ where: { mediaAssetId } });
      await tx.mediaAssetTag.deleteMany({ where: { mediaAssetId } });

      if (level2Ids.length > 0) {
        await tx.mediaAssetCategory.createMany({
          data: level2Ids.map((level2Id) => ({ mediaAssetId, level2Id })),
          skipDuplicates: true,
        });
      }

      if (tagIds.length > 0) {
        await tx.mediaAssetTag.createMany({
          data: tagIds.map((tagId) => ({ mediaAssetId, tagId })),
          skipDuplicates: true,
        });
      }
    });

    return this.getMediaAssetTaxonomy(mediaAssetId);
  }

  async replaceCollectionTaxonomy(collectionIdRaw: string | bigint, payload: { level2Ids?: string[]; tagIds?: string[] }) {
    const collectionId = typeof collectionIdRaw === 'bigint' ? collectionIdRaw : BigInt(collectionIdRaw);
    const level2Ids = this.normalizeIds(payload.level2Ids);
    const tagIds = this.normalizeIds(payload.tagIds);

    await this.assertLevel2IdsExist(level2Ids);
    await this.assertTagIdsExist(tagIds);

    await this.prisma.$transaction(async (tx) => {
      await tx.collectionCategory.deleteMany({ where: { collectionId } });
      await tx.collectionTag.deleteMany({ where: { collectionId } });

      if (level2Ids.length > 0) {
        await tx.collectionCategory.createMany({
          data: level2Ids.map((level2Id) => ({ collectionId, level2Id })),
          skipDuplicates: true,
        });
      }

      if (tagIds.length > 0) {
        await tx.collectionTag.createMany({
          data: tagIds.map((tagId) => ({ collectionId, tagId })),
          skipDuplicates: true,
        });
      }
    });

    return this.getCollectionTaxonomy(collectionId);
  }

  async replaceChannelDefaultTaxonomy(channelIdRaw: string | bigint, payload: { level2Ids?: string[]; tagIds?: string[] }) {
    const channelId = typeof channelIdRaw === 'bigint' ? channelIdRaw : BigInt(channelIdRaw);
    const level2Ids = this.normalizeIds(payload.level2Ids);
    const tagIds = this.normalizeIds(payload.tagIds);

    await this.assertLevel2IdsExist(level2Ids);
    await this.assertTagIdsExist(tagIds);

    await this.prisma.$transaction(async (tx) => {
      await tx.channelDefaultCategory.deleteMany({ where: { channelId } });
      await tx.channelDefaultTag.deleteMany({ where: { channelId } });

      if (level2Ids.length > 0) {
        await tx.channelDefaultCategory.createMany({
          data: level2Ids.map((level2Id) => ({ channelId, level2Id })),
          skipDuplicates: true,
        });
      }

      if (tagIds.length > 0) {
        await tx.channelDefaultTag.createMany({
          data: tagIds.map((tagId) => ({ channelId, tagId })),
          skipDuplicates: true,
        });
      }
    });

    return this.getChannelDefaultTaxonomy(channelId);
  }

  async getMediaAssetTaxonomy(mediaAssetIdRaw: string | bigint) {
    const mediaAssetId = typeof mediaAssetIdRaw === 'bigint' ? mediaAssetIdRaw : BigInt(mediaAssetIdRaw);
    const [categories, tags] = await Promise.all([
      this.prisma.mediaAssetCategory.findMany({
        where: { mediaAssetId },
        include: { level2: { include: { level1: true } } },
        orderBy: [{ level2: { sort: 'asc' } }, { level2: { name: 'asc' } }],
      }),
      this.prisma.mediaAssetTag.findMany({
        where: { mediaAssetId },
        include: { tag: true },
        orderBy: [{ tag: { sort: 'asc' } }, { tag: { name: 'asc' } }],
      }),
    ]);

    return {
      level2: categories.map((row) => this.serializeCategory(row.level2)),
      tags: tags.map((row) => this.serializeTag(row.tag)),
    };
  }

  async getCollectionTaxonomy(collectionIdRaw: string | bigint) {
    const collectionId = typeof collectionIdRaw === 'bigint' ? collectionIdRaw : BigInt(collectionIdRaw);
    const [categories, tags] = await Promise.all([
      this.prisma.collectionCategory.findMany({
        where: { collectionId },
        include: { level2: { include: { level1: true } } },
        orderBy: [{ level2: { sort: 'asc' } }, { level2: { name: 'asc' } }],
      }),
      this.prisma.collectionTag.findMany({
        where: { collectionId },
        include: { tag: true },
        orderBy: [{ tag: { sort: 'asc' } }, { tag: { name: 'asc' } }],
      }),
    ]);

    return {
      level2: categories.map((row) => this.serializeCategory(row.level2)),
      tags: tags.map((row) => this.serializeTag(row.tag)),
    };
  }

  async getChannelDefaultTaxonomy(channelIdRaw: string | bigint) {
    const channelId = typeof channelIdRaw === 'bigint' ? channelIdRaw : BigInt(channelIdRaw);
    const [categories, tags] = await Promise.all([
      this.prisma.channelDefaultCategory.findMany({
        where: { channelId },
        include: { level2: { include: { level1: true } } },
        orderBy: [{ level2: { sort: 'asc' } }, { level2: { name: 'asc' } }],
      }),
      this.prisma.channelDefaultTag.findMany({
        where: { channelId },
        include: { tag: true },
        orderBy: [{ tag: { sort: 'asc' } }, { tag: { name: 'asc' } }],
      }),
    ]);

    return {
      level2: categories.map((row) => this.serializeCategory(row.level2)),
      tags: tags.map((row) => this.serializeTag(row.tag)),
    };
  }

  private normalizeIds(values?: string[]) {
    const normalized = (values ?? [])
      .map((value) => String(value).trim())
      .filter((value) => /^\d+$/.test(value))
      .map((value) => BigInt(value));

    return Array.from(new Set(normalized.map((value) => value.toString()))).map((value) => BigInt(value));
  }

  private async assertLevel1Exists(level1Id: string) {
    const exists = await this.prisma.categoryLevel1.findUnique({
      where: { id: BigInt(level1Id) },
      select: { id: true },
    });

    if (!exists) {
      throw new NotFoundException(`category level1 not found: ${level1Id}`);
    }
  }

  private async assertLevel2IdsExist(level2Ids: bigint[]) {
    if (level2Ids.length === 0) return;

    const rows = await this.prisma.categoryLevel2.findMany({
      where: { id: { in: level2Ids } },
      select: { id: true },
    });

    this.assertIdsResolved('level2Ids', level2Ids, rows.map((row) => row.id));
  }

  private async assertTagIdsExist(tagIds: bigint[]) {
    if (tagIds.length === 0) return;

    const rows = await this.prisma.contentTag.findMany({
      where: { id: { in: tagIds } },
      select: { id: true },
    });

    this.assertIdsResolved('tagIds', tagIds, rows.map((row) => row.id));
  }

  private assertIdsResolved(field: string, expected: bigint[], actual: bigint[]) {
    const actualSet = new Set(actual.map((value) => value.toString()));
    const missing = expected.map((value) => value.toString()).filter((value) => !actualSet.has(value));
    if (missing.length > 0) {
      throw new BadRequestException(`${field} contains invalid ids: ${missing.join(', ')}`);
    }
  }

  private serializeCategory(row: {
    id: bigint;
    name: string;
    slug: string;
    sort: number;
    status: string;
    level1Id: bigint;
    level1?: { id: bigint; name: string; slug: string };
  }): TaxonomyCategoryView {
    if (!row.level1) {
      throw new BadRequestException('category level1 relation is required');
    }

    return {
      id: row.id.toString(),
      name: row.name,
      slug: row.slug,
      sort: row.sort,
      status: row.status,
      level1Id: row.level1Id.toString(),
      level1Name: row.level1.name,
      level1Slug: row.level1.slug,
    };
  }

  private serializeTag(row: {
    id: bigint;
    name: string;
    slug: string;
    sort: number;
    status: string;
    scope: string;
  }): TaxonomyTagView {
    return {
      id: row.id.toString(),
      name: row.name,
      slug: row.slug,
      sort: row.sort,
      status: row.status,
      scope: row.scope,
    };
  }
}

import { Injectable, Logger } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { SearchIndexOutboxService } from './search-index-outbox.service';

export interface BuiltSearchDocument {
  docId: string;
  docType: string;
  channelId: bigint;
  collectionId?: bigint | null;
  mediaAssetId?: bigint | null;
  episodeId?: bigint | null;
  title: string;
  originalTitle?: string | null;
  aliases: string[];
  actors: string[];
  directors: string[];
  genres: string[];
  keywords: string[];
  year?: number | null;
  region?: string | null;
  language?: string | null;
  description?: string | null;
  searchText: string;
  telegramMessageLink?: string | null;
  telegramMessageId?: bigint | null;
  publishedAt?: Date | null;
  sourceUpdatedAt: Date;
  ext?: Prisma.InputJsonValue | null;
}

@Injectable()
export class SearchDocumentBuilder {
  private readonly logger = new Logger(SearchDocumentBuilder.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly outboxService: SearchIndexOutboxService,
  ) {}

  async rebuildFromMediaAsset(mediaAssetId: bigint) {
    const asset = await this.prisma.mediaAsset.findUnique({
      where: { id: mediaAssetId },
      include: {
        channel: { select: { id: true, status: true } },
      },
    });

    if (!asset) return { skipped: true, reason: 'media asset not found' };

    const sourceMeta = this.parseObject(asset.sourceMeta);
    const title = this.pickTitle({
      explicit: this.pickString(sourceMeta, 'title'),
      fallback: asset.originalName,
    });

    const doc: BuiltSearchDocument = {
      docId: `asset:${asset.id}`,
      docType: 'asset',
      channelId: asset.channelId,
      mediaAssetId: asset.id,
      title,
      originalTitle: this.pickNullableString(sourceMeta, 'originalTitle'),
      aliases: this.normalizeStringArray(this.pickUnknown(sourceMeta, 'aliases')),
      actors: this.normalizeStringArray(this.pickUnknown(sourceMeta, 'actors')),
      directors: this.normalizeStringArray(this.pickUnknown(sourceMeta, 'directors')),
      genres: this.normalizeStringArray(this.pickUnknown(sourceMeta, 'genres')),
      keywords: this.normalizeStringArray(this.pickUnknown(sourceMeta, 'keywords')),
      year: this.pickNullableNumber(sourceMeta, 'year'),
      region: this.pickNullableString(sourceMeta, 'region'),
      language: this.pickNullableString(sourceMeta, 'language'),
      description: this.pickNullableString(sourceMeta, 'description') ?? asset.aiGeneratedCaption,
      telegramMessageLink: null,
      telegramMessageId: null,
      publishedAt: null,
      sourceUpdatedAt: asset.updatedAt,
      ext: sourceMeta as Prisma.InputJsonValue,
      searchText: this.composeSearchText([
        title,
        asset.originalName,
        this.pickNullableString(sourceMeta, 'originalTitle'),
        ...this.normalizeStringArray(this.pickUnknown(sourceMeta, 'aliases')),
        ...this.normalizeStringArray(this.pickUnknown(sourceMeta, 'actors')),
        ...this.normalizeStringArray(this.pickUnknown(sourceMeta, 'keywords')),
        this.pickNullableString(sourceMeta, 'description'),
        asset.aiGeneratedCaption,
      ]),
    };

    await this.upsertDocument(doc, asset.channel.status === 'active' && asset.status !== 'deleted');
    return { skipped: false, docId: doc.docId };
  }

  async rebuildFromCollectionEpisode(episodeId: bigint) {
    const episode = await this.prisma.collectionEpisode.findUnique({
      where: { id: episodeId },
      include: {
        collection: {
          include: {
            channel: { select: { status: true } },
          },
        },
        mediaAsset: true,
      },
    });

    if (!episode) return { skipped: true, reason: 'episode not found' };

    const sourceMeta = this.parseObject(episode.mediaAsset.sourceMeta);
    const collectionName = episode.collection.name;
    const episodeTitle = (episode.episodeTitle || episode.fileNameSnapshot || '').trim();
    const title = `${collectionName} 第${episode.episodeNo}集${episodeTitle ? ` ${episodeTitle}` : ''}`.trim();

    const doc: BuiltSearchDocument = {
      docId: `episode:${episode.id}`,
      docType: 'episode',
      channelId: episode.collection.channelId,
      collectionId: episode.collectionId,
      mediaAssetId: episode.mediaAssetId,
      episodeId: episode.id,
      title,
      originalTitle: this.pickNullableString(sourceMeta, 'originalTitle'),
      aliases: this.normalizeStringArray(this.pickUnknown(sourceMeta, 'aliases')),
      actors: this.normalizeStringArray(this.pickUnknown(sourceMeta, 'actors')),
      directors: this.normalizeStringArray(this.pickUnknown(sourceMeta, 'directors')),
      genres: this.normalizeStringArray(this.pickUnknown(sourceMeta, 'genres')),
      keywords: this.normalizeStringArray([
        ...this.normalizeStringArray(this.pickUnknown(sourceMeta, 'keywords')),
        collectionName,
        `第${episode.episodeNo}集`,
      ]),
      year: this.pickNullableNumber(sourceMeta, 'year'),
      region: this.pickNullableString(sourceMeta, 'region'),
      language: this.pickNullableString(sourceMeta, 'language'),
      description: this.pickNullableString(sourceMeta, 'description') ?? episode.mediaAsset.aiGeneratedCaption,
      telegramMessageLink: episode.telegramMessageLink,
      telegramMessageId: episode.telegramMessageId,
      publishedAt: episode.publishedAt,
      sourceUpdatedAt: episode.updatedAt,
      ext: {
        collectionName,
        episodeNo: episode.episodeNo,
        parseStatus: episode.parseStatus,
      },
      searchText: this.composeSearchText([
        title,
        collectionName,
        episode.episodeTitle,
        episode.fileNameSnapshot,
        ...this.normalizeStringArray(this.pickUnknown(sourceMeta, 'aliases')),
        ...this.normalizeStringArray(this.pickUnknown(sourceMeta, 'actors')),
        ...this.normalizeStringArray(this.pickUnknown(sourceMeta, 'keywords')),
        this.pickNullableString(sourceMeta, 'description'),
      ]),
    };

    await this.upsertDocument(
      doc,
      episode.collection.channel.status === 'active' && episode.collection.status === 'active',
    );
    return { skipped: false, docId: doc.docId };
  }

  async rebuildFromCollection(collectionId: bigint) {
    const collection = await this.prisma.collection.findUnique({
      where: { id: collectionId },
      include: {
        channel: { select: { id: true, status: true } },
        episodes: {
          orderBy: { episodeNo: 'asc' },
          select: {
            id: true,
            episodeNo: true,
            episodeTitle: true,
          },
        },
      },
    });

    if (!collection) return { skipped: true, reason: 'collection not found' };

    const episodeTitles = collection.episodes
      .slice(0, 20)
      .map((ep) => ep.episodeTitle || `第${ep.episodeNo}集`)
      .filter(Boolean);

    const doc: BuiltSearchDocument = {
      docId: `collection:${collection.id}`,
      docType: 'collection',
      channelId: collection.channelId,
      collectionId: collection.id,
      title: collection.name,
      originalTitle: null,
      aliases: this.normalizeStringArray(collection.slug ? [collection.slug] : []),
      actors: [],
      directors: [],
      genres: [],
      keywords: this.normalizeStringArray([collection.name, collection.slug || '', ...episodeTitles]),
      year: null,
      region: null,
      language: null,
      description: collection.description,
      telegramMessageLink: null,
      telegramMessageId: collection.indexMessageId,
      publishedAt: collection.lastBuiltAt,
      sourceUpdatedAt: collection.updatedAt,
      ext: {
        dirPath: collection.dirPath,
        navEnabled: collection.navEnabled,
        episodeCount: collection.episodes.length,
      },
      searchText: this.composeSearchText([
        collection.name,
        collection.slug,
        collection.description,
        ...episodeTitles,
      ]),
    };

    await this.upsertDocument(doc, collection.channel.status === 'active' && collection.status === 'active');
    return { skipped: false, docId: doc.docId };
  }

  async markDeleted(docId: string) {
    await this.prisma.searchDocument.updateMany({
      where: { docId },
      data: {
        isDeleted: true,
        isActive: false,
        updatedAt: new Date(),
      },
    });
    await this.outboxService.enqueue(docId, 'delete');
  }

  private async upsertDocument(doc: BuiltSearchDocument, isActive: boolean) {
    await this.prisma.searchDocument.upsert({
      where: { docId: doc.docId },
      create: {
        docId: doc.docId,
        docType: doc.docType,
        channelId: doc.channelId,
        collectionId: doc.collectionId ?? null,
        mediaAssetId: doc.mediaAssetId ?? null,
        episodeId: doc.episodeId ?? null,
        title: doc.title,
        originalTitle: doc.originalTitle ?? null,
        aliases: doc.aliases,
        actors: doc.actors,
        directors: doc.directors,
        genres: doc.genres,
        keywords: doc.keywords,
        year: doc.year ?? null,
        region: doc.region ?? null,
        language: doc.language ?? null,
        description: doc.description ?? null,
        searchText: doc.searchText,
        telegramMessageLink: doc.telegramMessageLink ?? null,
        telegramMessageId: doc.telegramMessageId ?? null,
        publishedAt: doc.publishedAt ?? null,
        ext: doc.ext ?? Prisma.JsonNull,
        sourceUpdatedAt: doc.sourceUpdatedAt,
        indexedAt: new Date(),
        isActive,
        isDeleted: false,
      },
      update: {
        docType: doc.docType,
        channelId: doc.channelId,
        collectionId: doc.collectionId ?? null,
        mediaAssetId: doc.mediaAssetId ?? null,
        episodeId: doc.episodeId ?? null,
        title: doc.title,
        originalTitle: doc.originalTitle ?? null,
        aliases: doc.aliases,
        actors: doc.actors,
        directors: doc.directors,
        genres: doc.genres,
        keywords: doc.keywords,
        year: doc.year ?? null,
        region: doc.region ?? null,
        language: doc.language ?? null,
        description: doc.description ?? null,
        searchText: doc.searchText,
        telegramMessageLink: doc.telegramMessageLink ?? null,
        telegramMessageId: doc.telegramMessageId ?? null,
        publishedAt: doc.publishedAt ?? null,
        ext: doc.ext ?? Prisma.JsonNull,
        sourceUpdatedAt: doc.sourceUpdatedAt,
        indexedAt: new Date(),
        isActive,
        isDeleted: false,
      },
    });

    await this.outboxService.enqueue(doc.docId, 'upsert');
    this.logger.debug(`search document upserted: ${doc.docId}`);
  }

  private pickTitle(args: { explicit?: string | null; fallback: string }) {
    const explicit = (args.explicit || '').trim();
    if (explicit) return explicit;
    return args.fallback.replace(/\.[^.]+$/, '').trim() || args.fallback;
  }

  private composeSearchText(parts: Array<string | null | undefined>) {
    return parts
      .map((p) => (p || '').trim())
      .filter(Boolean)
      .join(' | ')
      .slice(0, 8000);
  }

  private normalizeStringArray(input: unknown): string[] {
    if (!Array.isArray(input)) {
      if (typeof input === 'string') {
        return this.normalizeStringArray(
          input
            .split(/[、,，;；|/]/g)
            .map((s) => s.trim())
            .filter(Boolean),
        );
      }
      return [];
    }

    return Array.from(
      new Set(
        input
          .map((s) => (typeof s === 'string' ? s.trim() : ''))
          .filter(Boolean),
      ),
    );
  }

  private parseObject(value: unknown): Record<string, unknown> {
    if (!value || typeof value !== 'object') return {};
    return value as Record<string, unknown>;
  }

  private pickUnknown(source: Record<string, unknown>, key: string): unknown {
    return source[key];
  }

  private pickString(source: Record<string, unknown>, key: string): string | null {
    const raw = source[key];
    return typeof raw === 'string' ? raw : null;
  }

  private pickNullableString(source: Record<string, unknown>, key: string): string | null {
    const raw = source[key];
    if (typeof raw !== 'string') return null;
    const text = raw.trim();
    return text || null;
  }

  private pickNullableNumber(source: Record<string, unknown>, key: string): number | null {
    const raw = source[key];
    if (typeof raw === 'number' && Number.isFinite(raw)) return Math.trunc(raw);
    if (typeof raw === 'string') {
      const parsed = Number.parseInt(raw, 10);
      if (Number.isFinite(parsed)) return parsed;
    }
    return null;
  }
}

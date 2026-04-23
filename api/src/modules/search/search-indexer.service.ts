import { Injectable, Logger } from '@nestjs/common';
import { Client as OpenSearchClient } from '@opensearch-project/opensearch';
import { PrismaService } from '../prisma/prisma.service';
import { SearchIndexOutboxService } from './search-index-outbox.service';

@Injectable()
export class SearchIndexerService {
  private readonly logger = new Logger(SearchIndexerService.name);
  private readonly openSearchEnabled: boolean;
  private readonly openSearchIndex: string;
  private readonly openSearchReadAlias: string;
  private readonly openSearchWriteAlias: string;
  private readonly client: OpenSearchClient | null;

  constructor(
    private readonly prisma: PrismaService,
    private readonly outboxService: SearchIndexOutboxService,
  ) {
    const node = this.readString('OPENSEARCH_NODE', 'http://localhost:9200');
    this.openSearchEnabled = this.readBoolean('OPENSEARCH_ENABLED', true);
    this.openSearchIndex = this.readString('OPENSEARCH_INDEX', 'search_documents_v2');
    this.openSearchReadAlias = this.readString('OPENSEARCH_INDEX_READ', 'search_documents_read');
    this.openSearchWriteAlias = this.readString('OPENSEARCH_INDEX_WRITE', 'search_documents_write');

    if (!this.openSearchEnabled) {
      this.client = null;
      this.logger.warn('[search-indexer] OPENSEARCH_ENABLED=false，索引同步已禁用');
      return;
    }

    const username = this.readOptionalString('OPENSEARCH_USERNAME');
    const password = this.readOptionalString('OPENSEARCH_PASSWORD');
    const allowSelfSigned = this.readBoolean('OPENSEARCH_ALLOW_SELF_SIGNED', false);

    this.client = new OpenSearchClient({
      node,
      auth: username && password ? { username, password } : undefined,
      ssl: allowSelfSigned ? { rejectUnauthorized: false } : undefined,
    });
  }

  async processBatch(limit = 50) {
    const items = await this.outboxService.claimPendingBatch(limit);
    if (items.length === 0) return { processed: 0, success: 0, failed: 0 };

    let success = 0;
    let failed = 0;

    for (const item of items) {
      try {
        await this.syncToSearchEngine(item.docId, item.op);
        await this.outboxService.markDone(item.id);
        success += 1;
      } catch (error) {
        await this.outboxService.markFailed(item.id, item.attempt, error);
        failed += 1;
      }
    }

    this.logger.log(`search outbox processed: ${items.length}, success=${success}, failed=${failed}`);
    return { processed: items.length, success, failed };
  }

  private async syncToSearchEngine(docId: string, op: string) {
    if (!this.openSearchEnabled || !this.client) {
      this.logger.debug(`[search-indexer] skip sync (disabled), docId=${docId}, op=${op}`);
      return;
    }

    if (op === 'delete') {
      await this.client.delete(
        {
          index: this.openSearchWriteAlias,
          id: docId,
        },
        {
          ignore: [404],
        },
      );
      this.logger.debug(`delete doc from search engine(write alias): ${docId}`);
      return;
    }

    const doc = await this.prisma.searchDocument.findUnique({
      where: { docId },
    });

    if (!doc) {
      this.logger.warn(`missing search document: ${docId}`);
      return;
    }

    const [categories, tags] = await Promise.all([
      this.prisma.$queryRaw<Array<{ level1_id: bigint; level1_name: string; level2_id: bigint; level2_name: string }>>`
        SELECT
          l1.id AS level1_id,
          l1.name AS level1_name,
          l2.id AS level2_id,
          l2.name AS level2_name
        FROM search_document_categories sdc
        INNER JOIN category_level2 l2 ON l2.id = sdc.level2_id
        INNER JOIN category_level1 l1 ON l1.id = l2.level1_id
        WHERE sdc.search_document_id = ${doc.id}
      `,
      this.prisma.$queryRaw<Array<{ tag_id: bigint; tag_name: string }>>`
        SELECT
          ct.id AS tag_id,
          ct.name AS tag_name
        FROM search_document_tags sdt
        INNER JOIN content_tags ct ON ct.id = sdt.tag_id
        WHERE sdt.search_document_id = ${doc.id}
      `,
    ]);

    const categoryLevel1Ids = Array.from(new Set(categories.map((row) => Number(row.level1_id))));
    const categoryLevel1Names = Array.from(new Set(categories.map((row) => row.level1_name)));
    const categoryLevel2Ids = Array.from(new Set(categories.map((row) => Number(row.level2_id))));
    const categoryLevel2Names = Array.from(new Set(categories.map((row) => row.level2_name)));
    const tagIds = Array.from(new Set(tags.map((row) => Number(row.tag_id))));
    const tagNames = Array.from(new Set(tags.map((row) => row.tag_name)));

    const body = {
      doc_id: doc.docId,
      doc_type: doc.docType,
      schema_version: doc.schemaVersion,
      channel_id: Number(doc.channelId),
      collection_id: doc.collectionId ? Number(doc.collectionId) : null,
      media_asset_id: doc.mediaAssetId ? Number(doc.mediaAssetId) : null,
      episode_id: doc.episodeId ? Number(doc.episodeId) : null,
      title: doc.title,
      original_title: doc.originalTitle,
      aliases: doc.aliases,
      actors: doc.actors,
      directors: doc.directors,
      genres: doc.genres,
      keywords: doc.keywords,
      year: doc.year,
      region: doc.region,
      language: doc.language,
      description: doc.description,
      search_text: doc.searchText,
      telegram_message_link: doc.telegramMessageLink,
      telegram_message_id: doc.telegramMessageId ? doc.telegramMessageId.toString() : null,
      published_at: doc.publishedAt?.toISOString() ?? null,
      quality_score: Number(doc.qualityScore),
      popularity_score: Number(doc.popularityScore),
      manual_weight: Number(doc.manualWeight),
      visibility: doc.visibility,
      category_level1_ids: categoryLevel1Ids,
      category_level1_names: categoryLevel1Names,
      category_level2_ids: categoryLevel2Ids,
      category_level2_names: categoryLevel2Names,
      tag_ids: tagIds,
      tag_names: tagNames,
      is_active: doc.isActive,
      is_deleted: doc.isDeleted,
      ext: doc.ext,
      source_updated_at: doc.sourceUpdatedAt.toISOString(),
      indexed_at: doc.indexedAt.toISOString(),
      updated_at: doc.updatedAt.toISOString(),
      created_at: doc.createdAt.toISOString(),
    };

    await this.client.index({
      index: this.openSearchWriteAlias,
      id: docId,
      body,
      refresh: false,
    });

    this.logger.debug(`upsert doc to search engine(write alias): ${docId}`);
  }

  async searchViaReadAlias(args: {
    keyword: string;
    channelIds: bigint[];
    limit: number;
    offset: number;
    docTypes: string[];
    visibilityValues: string[];
  }) {
    if (!this.openSearchEnabled || !this.client) {
      return { enabled: false as const, results: [], total: 0 };
    }

    const result = await this.client.search({
      index: this.openSearchReadAlias,
      body: {
        from: args.offset,
        size: args.limit,
        query: {
          bool: {
            must: [
              {
                multi_match: {
                  query: args.keyword,
                  type: 'best_fields',
                  fields: ['title^5', 'aliases^3', 'actors^2', 'keywords^1.5', 'description', 'search_text'],
                },
              },
            ],
            filter: [
              { term: { is_active: true } },
              { term: { is_deleted: false } },
              { terms: { channel_id: args.channelIds.map((id) => Number(id)) } },
              { terms: { doc_type: args.docTypes } },
              { terms: { visibility: args.visibilityValues } },
            ],
          },
        },
        sort: [
          { _score: { order: 'desc' } },
          { published_at: { order: 'desc', unmapped_type: 'date' } },
        ],
      },
    });

    const body: any = result.body;
    const hits = body?.hits?.hits ?? [];
    const totalRaw = body?.hits?.total?.value ?? body?.hits?.total ?? 0;

    return {
      enabled: true as const,
      results: hits.map((hit: any) => ({
        ...hit._source,
        score: hit._score,
      })),
      total: Number(totalRaw),
    };
  }

  async searchHotViaReadAlias(args: {
    channelIds: bigint[];
    limit: number;
    offset: number;
    periodDays: number;
    docTypes: string[];
    visibilityValues: string[];
  }) {
    if (!this.openSearchEnabled || !this.client) {
      return { enabled: false as const, results: [], total: 0 };
    }

    const result = await this.client.search({
      index: this.openSearchReadAlias,
      body: {
        from: args.offset,
        size: args.limit,
        query: {
          bool: {
            filter: [
              { term: { is_active: true } },
              { term: { is_deleted: false } },
              { terms: { channel_id: args.channelIds.map((id) => Number(id)) } },
              { terms: { doc_type: args.docTypes } },
              { terms: { visibility: args.visibilityValues } },
              {
                range: {
                  published_at: {
                    gte: `now-${args.periodDays}d/d`,
                  },
                },
              },
            ],
          },
        },
        sort: [
          { popularity_score: { order: 'desc', unmapped_type: 'float' } },
          { published_at: { order: 'desc', unmapped_type: 'date' } },
          { quality_score: { order: 'desc', unmapped_type: 'float' } },
        ],
      },
    });

    const body: any = result.body;
    const hits = body?.hits?.hits ?? [];
    const totalRaw = body?.hits?.total?.value ?? body?.hits?.total ?? 0;

    return {
      enabled: true as const,
      results: hits.map((hit: any) => ({
        ...hit._source,
        score: hit._score,
      })),
      total: Number(totalRaw),
    };
  }

  async searchByTagViaReadAlias(args: {
    channelIds: bigint[];
    limit: number;
    offset: number;
    tagId?: number;
    tagName?: string;
    level1Id?: number;
    docTypes: string[];
    visibilityValues: string[];
  }) {
    if (!this.openSearchEnabled || !this.client) {
      return { enabled: false as const, results: [], total: 0 };
    }

    const hasTagId = typeof args.tagId === 'number' && Number.isFinite(args.tagId) && args.tagId > 0;
    const normalizedTagName = String(args.tagName || '').trim();
    if (!hasTagId && !normalizedTagName) {
      return { enabled: false as const, results: [], total: 0 };
    }

    const filter: Array<Record<string, unknown>> = [
      { term: { is_active: true } },
      { term: { is_deleted: false } },
      { terms: { channel_id: args.channelIds.map((id) => Number(id)) } },
      { terms: { doc_type: args.docTypes } },
      { terms: { visibility: args.visibilityValues } },
    ];

    if (hasTagId) {
      filter.push({ term: { tag_ids: Number(args.tagId) } });
    } else {
      filter.push({ term: { 'tag_names.keyword': normalizedTagName } });
    }

    if (typeof args.level1Id === 'number' && Number.isFinite(args.level1Id) && args.level1Id > 0) {
      filter.push({ term: { category_level1_ids: Number(args.level1Id) } });
    }

    const result = await this.client.search({
      index: this.openSearchReadAlias,
      body: {
        from: args.offset,
        size: args.limit,
        query: {
          bool: {
            filter,
          },
        },
        sort: [
          { popularity_score: { order: 'desc', unmapped_type: 'float' } },
          { published_at: { order: 'desc', unmapped_type: 'date' } },
          { quality_score: { order: 'desc', unmapped_type: 'float' } },
        ],
      },
    });

    const body: any = result.body;
    const hits = body?.hits?.hits ?? [];
    const totalRaw = body?.hits?.total?.value ?? body?.hits?.total ?? 0;

    return {
      enabled: true as const,
      results: hits.map((hit: any) => ({
        ...hit._source,
        score: hit._score,
      })),
      total: Number(totalRaw),
    };
  }

  async switchAliases(targetIndex: string) {
    if (!this.openSearchEnabled || !this.client) {
      return {
        enabled: false,
        message: 'OPENSEARCH_NODE 未配置，跳过别名切换',
      };
    }

    const normalizedIndex = targetIndex.trim();
    if (!normalizedIndex) {
      throw new Error('targetIndex 不能为空');
    }

    const exists = await this.client.indices.exists({ index: normalizedIndex });
    if (!exists.body) {
      throw new Error(`目标索引不存在: ${normalizedIndex}`);
    }

    const readAliasIndices = await this.getIndicesByAlias(this.openSearchReadAlias);
    const writeAliasIndices = await this.getIndicesByAlias(this.openSearchWriteAlias);

    const actions: Array<Record<string, unknown>> = [];

    for (const index of readAliasIndices) {
      if (index !== normalizedIndex) {
        actions.push({ remove: { index, alias: this.openSearchReadAlias } });
      }
    }

    for (const index of writeAliasIndices) {
      if (index !== normalizedIndex) {
        actions.push({ remove: { index, alias: this.openSearchWriteAlias } });
      }
    }

    actions.push({ add: { index: normalizedIndex, alias: this.openSearchReadAlias } });
    actions.push({ add: { index: normalizedIndex, alias: this.openSearchWriteAlias } });

    await this.client.indices.updateAliases({
      body: {
        actions,
      },
    });

    return {
      enabled: true,
      switchedTo: normalizedIndex,
      readAlias: this.openSearchReadAlias,
      writeAlias: this.openSearchWriteAlias,
      previousReadAliasIndices: readAliasIndices,
      previousWriteAliasIndices: writeAliasIndices,
    };
  }

  async initializeOpenSearchIndex() {
    if (!this.openSearchEnabled || !this.client) {
      return {
        enabled: false,
        message: 'OPENSEARCH_NODE 未配置，跳过初始化',
      };
    }

    const indexExists = await this.client.indices.exists({ index: this.openSearchIndex });

    if (!indexExists.body) {
      await this.client.indices.create({
        index: this.openSearchIndex,
        body: {
          settings: {
            analysis: {
              analyzer: {
                default: {
                  type: 'custom',
                  tokenizer: 'standard',
                  filter: ['lowercase'],
                },
                search_analyzer: {
                  type: 'custom',
                  tokenizer: 'standard',
                  filter: ['lowercase'],
                },
              },
            },
            number_of_shards: 1,
            number_of_replicas: 1,
          },
          mappings: {
            dynamic: 'true',
            properties: {
              doc_id: { type: 'keyword' },
              doc_type: { type: 'keyword' },
              schema_version: { type: 'integer' },
              channel_id: { type: 'long' },
              collection_id: { type: 'long' },
              media_asset_id: { type: 'long' },
              episode_id: { type: 'long' },
              title: {
                type: 'text',
                analyzer: 'standard',
                search_analyzer: 'standard',
                fields: {
                  keyword: { type: 'keyword', ignore_above: 256 },
                },
              },
              original_title: {
                type: 'text',
                analyzer: 'standard',
                search_analyzer: 'standard',
              },
              aliases: {
                type: 'text',
                analyzer: 'standard',
                search_analyzer: 'standard',
                fields: {
                  keyword: { type: 'keyword', ignore_above: 256 },
                },
              },
              actors: {
                type: 'text',
                analyzer: 'standard',
                search_analyzer: 'standard',
                fields: {
                  keyword: { type: 'keyword', ignore_above: 256 },
                },
              },
              directors: { type: 'text', analyzer: 'standard', search_analyzer: 'standard' },
              genres: { type: 'keyword' },
              keywords: { type: 'text', analyzer: 'standard', search_analyzer: 'standard' },
              year: { type: 'integer' },
              region: { type: 'keyword' },
              language: { type: 'keyword' },
              description: { type: 'text', analyzer: 'standard', search_analyzer: 'standard' },
              search_text: { type: 'text', analyzer: 'standard', search_analyzer: 'standard' },
              telegram_message_link: { type: 'keyword', index: false },
              telegram_message_id: { type: 'keyword' },
              published_at: { type: 'date' },
              quality_score: { type: 'float' },
              popularity_score: { type: 'float' },
              manual_weight: { type: 'float' },
              visibility: { type: 'keyword' },
              category_level1_ids: { type: 'long' },
              category_level1_names: { type: 'keyword' },
              category_level2_ids: { type: 'long' },
              category_level2_names: { type: 'keyword' },
              tag_ids: { type: 'long' },
              tag_names: {
                type: 'text',
                analyzer: 'standard',
                search_analyzer: 'standard',
                fields: {
                  keyword: { type: 'keyword', ignore_above: 256 },
                },
              },
              is_active: { type: 'boolean' },
              is_deleted: { type: 'boolean' },
              ext: { type: 'object', enabled: false },
              source_updated_at: { type: 'date' },
              indexed_at: { type: 'date' },
              updated_at: { type: 'date' },
              created_at: { type: 'date' },
            },
          },
        },
      });
    }

    await this.client.indices.updateAliases({
      body: {
        actions: [
          { add: { index: this.openSearchIndex, alias: this.openSearchReadAlias } },
          { add: { index: this.openSearchIndex, alias: this.openSearchWriteAlias } },
        ],
      },
    });

    return {
      enabled: true,
      index: this.openSearchIndex,
      readAlias: this.openSearchReadAlias,
      writeAlias: this.openSearchWriteAlias,
      created: !indexExists.body,
    };
  }

  private async getIndicesByAlias(alias: string): Promise<string[]> {
    if (!this.client) return [];

    try {
      const resp = await this.client.indices.getAlias({ name: alias });
      const body = resp.body as Record<string, unknown>;
      return Object.keys(body || {});
    } catch (error: any) {
      const statusCode = error?.meta?.statusCode;
      if (statusCode === 404) return [];
      throw error;
    }
  }

  private readString(key: string, fallback: string): string {
    const raw = process.env[key];
    const normalized = typeof raw === 'string' ? raw.trim() : '';
    return normalized || fallback;
  }

  private readOptionalString(key: string): string | undefined {
    const raw = process.env[key];
    const normalized = typeof raw === 'string' ? raw.trim() : '';
    return normalized || undefined;
  }

  private readBoolean(key: string, fallback: boolean): boolean {
    const raw = process.env[key];
    if (raw == null) return fallback;
    const normalized = raw.trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    return fallback;
  }
}

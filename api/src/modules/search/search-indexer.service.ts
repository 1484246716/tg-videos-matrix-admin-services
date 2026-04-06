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
    const node = process.env.OPENSEARCH_NODE?.trim();
    this.openSearchEnabled = Boolean(node);
    this.openSearchIndex = (process.env.OPENSEARCH_INDEX || 'search_documents_v1').trim();
    this.openSearchReadAlias = (process.env.OPENSEARCH_INDEX_READ || 'search_documents_read').trim();
    this.openSearchWriteAlias = (process.env.OPENSEARCH_INDEX_WRITE || 'search_documents_write').trim();

    if (!node) {
      this.client = null;
      this.logger.warn('[search-indexer] OPENSEARCH_NODE 未配置，索引同步将仅记录日志');
      return;
    }

    const username = process.env.OPENSEARCH_USERNAME?.trim();
    const password = process.env.OPENSEARCH_PASSWORD?.trim();

    this.client = new OpenSearchClient({
      node,
      auth: username && password ? { username, password } : undefined,
      ssl: process.env.OPENSEARCH_ALLOW_SELF_SIGNED === 'true' ? { rejectUnauthorized: false } : undefined,
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
    tagName?: string;
  }) {
    if (!this.openSearchEnabled || !this.client || !args.tagName) {
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
                  query: args.tagName,
                  type: 'best_fields',
                  fields: ['genres^4', 'keywords^3', 'title^1.5', 'description'],
                },
              },
            ],
            filter: [
              { term: { is_active: true } },
              { term: { is_deleted: false } },
              { terms: { channel_id: args.channelIds.map((id) => Number(id)) } },
            ],
          },
        },
        sort: [
          { _score: { order: 'desc' } },
          { popularity_score: { order: 'desc', unmapped_type: 'float' } },
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
                  tokenizer: 'ik_max_word',
                  filter: ['lowercase'],
                },
                search_analyzer: {
                  type: 'custom',
                  tokenizer: 'ik_smart',
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
                analyzer: 'ik_max_word',
                search_analyzer: 'ik_smart',
                fields: {
                  keyword: { type: 'keyword', ignore_above: 256 },
                },
              },
              original_title: {
                type: 'text',
                analyzer: 'ik_max_word',
                search_analyzer: 'ik_smart',
              },
              aliases: {
                type: 'text',
                analyzer: 'ik_max_word',
                search_analyzer: 'ik_smart',
                fields: {
                  keyword: { type: 'keyword', ignore_above: 256 },
                },
              },
              actors: {
                type: 'text',
                analyzer: 'ik_max_word',
                search_analyzer: 'ik_smart',
                fields: {
                  keyword: { type: 'keyword', ignore_above: 256 },
                },
              },
              directors: { type: 'text', analyzer: 'ik_max_word', search_analyzer: 'ik_smart' },
              genres: { type: 'keyword' },
              keywords: { type: 'text', analyzer: 'ik_max_word', search_analyzer: 'ik_smart' },
              year: { type: 'integer' },
              region: { type: 'keyword' },
              language: { type: 'keyword' },
              description: { type: 'text', analyzer: 'ik_max_word', search_analyzer: 'ik_smart' },
              search_text: { type: 'text', analyzer: 'ik_max_word', search_analyzer: 'ik_smart' },
              telegram_message_link: { type: 'keyword', index: false },
              telegram_message_id: { type: 'keyword' },
              published_at: { type: 'date' },
              quality_score: { type: 'float' },
              popularity_score: { type: 'float' },
              manual_weight: { type: 'float' },
              visibility: { type: 'keyword' },
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
}

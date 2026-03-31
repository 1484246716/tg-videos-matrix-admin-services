import { BadRequestException, Injectable, InternalServerErrorException, NotFoundException } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { Queue } from 'bullmq';
import IORedis from 'ioredis';
import { mkdir, rm } from 'node:fs/promises';
import { normalize, resolve } from 'node:path';
import { PrismaService } from '../prisma/prisma.service';

type SaveCollectionDto = {
  channelId: string;
  name: string;
  slug?: string;
  dirPath: string;
  status: 'active' | 'paused' | 'archived';
  sortOrder: number;
  navEnabled: boolean;
  navPageSize: number;
  templateText?: string;
  inheritChannelOrderConfig?: boolean;
  collectionDispatchGateEnabled?: boolean;
  collectionHeadBypassEnabled?: boolean;
  collectionHeadBypassMinutes?: number;
  collectionGapPolicy?: 'strict' | 'allow_gap';
  collectionAllowedGapSize?: number;
};

const DEFAULT_COLLECTION_STATUS = 'active' as const;
const DEFAULT_COLLECTION_ORDER_CONFIG = {
  inheritChannelOrderConfig: true,
  collectionDispatchGateEnabled: true,
  collectionHeadBypassEnabled: false,
  collectionHeadBypassMinutes: 180,
  collectionGapPolicy: 'strict' as const,
  collectionAllowedGapSize: 0,
};

function normalizeCollectionName(name: string) {
  return name
    .normalize('NFKC')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseStoredPageMessageIds(raw: unknown) {
  if (!Array.isArray(raw)) return [] as number[];
  return raw.map((item) => Number(item)).filter((item) => Number.isInteger(item) && item > 0);
}

type CollectionNavState = {
  indexMessageId: number | null;
  indexPageMessageIds: number[];
  detailMessageIds: Record<string, number>;
  detailPageMessageIds: Record<string, number[]>;
};

function parseCollectionNavState(rawNavReplyMarkup: unknown): CollectionNavState {
  if (!rawNavReplyMarkup || typeof rawNavReplyMarkup !== 'object' || Array.isArray(rawNavReplyMarkup)) {
    return { indexMessageId: null, indexPageMessageIds: [], detailMessageIds: {}, detailPageMessageIds: {} };
  }

  const container = rawNavReplyMarkup as Record<string, unknown>;
  const state =
    container.__collectionNavState && typeof container.__collectionNavState === 'object'
      ? (container.__collectionNavState as Record<string, unknown>)
      : null;

  if (!state) return { indexMessageId: null, indexPageMessageIds: [], detailMessageIds: {}, detailPageMessageIds: {} };

  const indexMessageIdRaw = state.indexMessageId;
  const indexMessageId =
    typeof indexMessageIdRaw === 'number' && Number.isInteger(indexMessageIdRaw) && indexMessageIdRaw > 0
      ? indexMessageIdRaw
      : null;

  const indexPageMessageIds = parseStoredPageMessageIds(state.indexPageMessageIds);

  const detailRaw = state.detailMessageIds;
  const detailMessageIds: Record<string, number> = {};
  if (detailRaw && typeof detailRaw === 'object' && !Array.isArray(detailRaw)) {
    for (const [key, val] of Object.entries(detailRaw as Record<string, unknown>)) {
      const n = Number(val);
      if (Number.isInteger(n) && n > 0) {
        detailMessageIds[key] = n;
      }
    }
  }

  const detailPageRaw = state.detailPageMessageIds;
  const detailPageMessageIds: Record<string, number[]> = {};
  if (detailPageRaw && typeof detailPageRaw === 'object' && !Array.isArray(detailPageRaw)) {
    for (const [key, val] of Object.entries(detailPageRaw as Record<string, unknown>)) {
      const ids = parseStoredPageMessageIds(val);
      if (ids.length > 0) {
        detailPageMessageIds[key] = ids;
      }
    }
  }

  for (const [key, firstId] of Object.entries(detailMessageIds)) {
    if (!detailPageMessageIds[key] || detailPageMessageIds[key].length === 0) {
      detailPageMessageIds[key] = [firstId];
    }
  }

  return { indexMessageId, indexPageMessageIds, detailMessageIds, detailPageMessageIds };
}

function sanitizeJsonForPrisma(value: unknown): unknown {
  if (value === null) return null;
  if (value === undefined) return undefined;

  if (Array.isArray(value)) {
    return value
      .map((item) => sanitizeJsonForPrisma(item))
      .filter((item) => item !== undefined);
  }

  if (typeof value === 'object') {
    const result: Record<string, unknown> = {};
    for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
      const sanitized = sanitizeJsonForPrisma(item);
      if (sanitized !== undefined) {
        result[key] = sanitized;
      }
    }
    return result;
  }

  return value;
}

function parsePositiveInteger(value: unknown, fallback: number, min = 0, max = Number.MAX_SAFE_INTEGER) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  const normalized = Math.floor(parsed);
  if (normalized < min || normalized > max) return fallback;
  return normalized;
}

function parseCollectionOrderConfig(rawExtConfig: unknown): {
  inheritChannelOrderConfig: boolean;
  collectionDispatchGateEnabled: boolean;
  collectionHeadBypassEnabled: boolean;
  collectionHeadBypassMinutes: number;
  collectionGapPolicy: 'strict' | 'allow_gap';
  collectionAllowedGapSize: number;
} {
  const extConfig =
    rawExtConfig && typeof rawExtConfig === 'object' && !Array.isArray(rawExtConfig)
      ? (rawExtConfig as Record<string, unknown>)
      : {};
  const order =
    extConfig.order && typeof extConfig.order === 'object' && !Array.isArray(extConfig.order)
      ? (extConfig.order as Record<string, unknown>)
      : {};

  return {
    inheritChannelOrderConfig:
      typeof order.inheritChannelOrderConfig === 'boolean'
        ? order.inheritChannelOrderConfig
        : DEFAULT_COLLECTION_ORDER_CONFIG.inheritChannelOrderConfig,
    collectionDispatchGateEnabled:
      typeof order.collectionDispatchGateEnabled === 'boolean'
        ? order.collectionDispatchGateEnabled
        : DEFAULT_COLLECTION_ORDER_CONFIG.collectionDispatchGateEnabled,
    collectionHeadBypassEnabled:
      typeof order.collectionHeadBypassEnabled === 'boolean'
        ? order.collectionHeadBypassEnabled
        : DEFAULT_COLLECTION_ORDER_CONFIG.collectionHeadBypassEnabled,
    collectionHeadBypassMinutes: parsePositiveInteger(
      order.collectionHeadBypassMinutes,
      DEFAULT_COLLECTION_ORDER_CONFIG.collectionHeadBypassMinutes,
      1,
      1440,
    ),
    collectionGapPolicy:
      order.collectionGapPolicy === 'allow_gap' || order.collectionGapPolicy === 'strict'
        ? order.collectionGapPolicy
        : DEFAULT_COLLECTION_ORDER_CONFIG.collectionGapPolicy,
    collectionAllowedGapSize: parsePositiveInteger(
      order.collectionAllowedGapSize,
      DEFAULT_COLLECTION_ORDER_CONFIG.collectionAllowedGapSize,
      0,
      20,
    ),
  };
}

function mergeCollectionOrderConfigIntoExtConfig(
  rawExtConfig: unknown,
  dto: Pick<
    SaveCollectionDto,
    | 'inheritChannelOrderConfig'
    | 'collectionDispatchGateEnabled'
    | 'collectionHeadBypassEnabled'
    | 'collectionHeadBypassMinutes'
    | 'collectionGapPolicy'
    | 'collectionAllowedGapSize'
  >,
) {
  const extConfig =
    rawExtConfig && typeof rawExtConfig === 'object' && !Array.isArray(rawExtConfig)
      ? { ...(rawExtConfig as Record<string, unknown>) }
      : {};
  const currentOrder =
    extConfig.order && typeof extConfig.order === 'object' && !Array.isArray(extConfig.order)
      ? { ...(extConfig.order as Record<string, unknown>) }
      : {};

  const nextOrder = {
    ...currentOrder,
    inheritChannelOrderConfig:
      typeof dto.inheritChannelOrderConfig === 'boolean'
        ? dto.inheritChannelOrderConfig
        : DEFAULT_COLLECTION_ORDER_CONFIG.inheritChannelOrderConfig,
    collectionDispatchGateEnabled:
      typeof dto.collectionDispatchGateEnabled === 'boolean'
        ? dto.collectionDispatchGateEnabled
        : DEFAULT_COLLECTION_ORDER_CONFIG.collectionDispatchGateEnabled,
    collectionHeadBypassEnabled:
      typeof dto.collectionHeadBypassEnabled === 'boolean'
        ? dto.collectionHeadBypassEnabled
        : DEFAULT_COLLECTION_ORDER_CONFIG.collectionHeadBypassEnabled,
    collectionHeadBypassMinutes: parsePositiveInteger(
      dto.collectionHeadBypassMinutes,
      DEFAULT_COLLECTION_ORDER_CONFIG.collectionHeadBypassMinutes,
      1,
      1440,
    ),
    collectionGapPolicy:
      dto.collectionGapPolicy === 'allow_gap' || dto.collectionGapPolicy === 'strict'
        ? dto.collectionGapPolicy
        : DEFAULT_COLLECTION_ORDER_CONFIG.collectionGapPolicy,
    collectionAllowedGapSize: parsePositiveInteger(
      dto.collectionAllowedGapSize,
      DEFAULT_COLLECTION_ORDER_CONFIG.collectionAllowedGapSize,
      0,
      20,
    ),
    updatedAt: new Date().toISOString(),
  };

  extConfig.order = nextOrder;
  return sanitizeJsonForPrisma(extConfig) as Prisma.InputJsonValue;
}

function mergeCollectionNavStateIntoReplyMarkup(rawNavReplyMarkup: unknown, state: CollectionNavState | null) {
  const base =
    rawNavReplyMarkup && typeof rawNavReplyMarkup === 'object' && !Array.isArray(rawNavReplyMarkup)
      ? { ...(rawNavReplyMarkup as Record<string, unknown>) }
      : {};

  if (!state) {
    delete (base as Record<string, unknown>).__collectionNavState;
    return sanitizeJsonForPrisma(base) as Record<string, unknown>;
  }

  (base as Record<string, unknown>).__collectionNavState = {
    indexMessageId: state.indexMessageId ?? null,
    indexPageMessageIds: state.indexPageMessageIds,
    detailMessageIds: state.detailMessageIds,
    detailPageMessageIds: state.detailPageMessageIds,
    updatedAt: new Date().toISOString(),
  };

  return sanitizeJsonForPrisma(base) as Record<string, unknown>;
}

function isTelegramMessageDeleteNotFoundError(error: { code?: string; message?: string } | null | undefined) {
  const code = error?.code ?? '';
  const message = (error?.message ?? '').toLowerCase();
  return code === 'TG_400' && message.includes('message to delete not found');
}

function parseCollectionMeta(sourceMeta: unknown) {
  if (!sourceMeta || typeof sourceMeta !== 'object') return null;
  const meta = sourceMeta as Record<string, unknown>;
  if (meta.isCollection !== true) return null;

  const collectionName = typeof meta.collectionName === 'string' ? meta.collectionName.trim() : '';
  const episodeNo =
    typeof meta.episodeNo === 'number'
      ? meta.episodeNo
      : typeof meta.episodeNo === 'string' && /^\d+$/.test(meta.episodeNo)
        ? Number(meta.episodeNo)
        : null;

  if (!collectionName || episodeNo === null) return null;

  return {
    collectionName,
    episodeNo,
  };
}

function getFileStem(name: string) {
  if (!name) return '';
  const base = name.replace(/^.*[\\/]/, '');
  const idx = base.lastIndexOf('.');
  return idx > 0 ? base.slice(0, idx) : base;
}

let searchIndexQueue: Queue | null = null;

function getSearchIndexQueue() {
  if (searchIndexQueue) return searchIndexQueue;

  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
  const connection = new IORedis(redisUrl, { maxRetriesPerRequest: null });
  connection.on('error', (error) => {
    console.error('[redis] collection search-index redis error:', error?.message ?? error);
  });

  searchIndexQueue = new Queue('q_search_index', {
    connection: connection as any,
  });

  return searchIndexQueue;
}

@Injectable()
export class CollectionService {
  constructor(private readonly prisma: PrismaService) {}

  private getTelegramBotApiBase() {
    return (
      (process.env.TELEGRAM_BOT_API_BASE || 'https://api.telegram.org').trim() ||
      'https://api.telegram.org'
    ).replace(/\/$/, '');
  }

  private getChannelsRootDir() {
    const raw = (process.env.CHANNELS_ROOT_DIR || './data/channels').trim();

    if (/^\/[a-zA-Z]/.test(raw)) {
      const driveRelative = raw.replace(/^\//, '');
      const workspaceRoot = resolve(process.cwd(), '..', '..');
      return resolve(workspaceRoot, driveRelative);
    }

    return resolve(raw);
  }

  private resolveChannelRelativePath(folderPath: string) {
    const normalizedInput = normalize(folderPath.trim().replace(/\\/g, '/'));
    return normalizedInput.replace(/^[\\/]+/, '');
  }

  private resolveCollectionFolderPath(args: { channelFolderPath: string; collectionName: string }) {
    const channelsRoot = this.getChannelsRootDir();
    const channelRel = this.resolveChannelRelativePath(args.channelFolderPath);
    return resolve(channelsRoot, channelRel, 'Collection', args.collectionName);
  }

  private async ensureCollectionFolderExists(args: { channelFolderPath: string; collectionName: string }) {
    const targetDir = this.resolveCollectionFolderPath(args);
    await mkdir(targetDir, { recursive: true });
    return targetDir;
  }

  private async removeCollectionFolderIfExists(args: { channelFolderPath: string; collectionName: string }) {
    const targetDir = this.resolveCollectionFolderPath(args);
    await rm(targetDir, { recursive: true, force: true });
    return targetDir;
  }

  private async deleteTelegramMessage(args: {
    botToken: string;
    chatId: string;
    messageId: number;
  }) {
    const response = await fetch(`${this.getTelegramBotApiBase()}/bot${args.botToken}/deleteMessage`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        chat_id: args.chatId,
        message_id: args.messageId,
      }),
    });

    const json = (await response.json().catch(() => ({}))) as {
      ok?: boolean;
      description?: string;
      error_code?: number;
    };

    if (!response.ok || !json.ok) {
      const errorCode = json.error_code ?? response.status;
      const description = json.description || `Telegram API 请求失败 (${response.status})`;
      throw {
        code: `TG_${errorCode}`,
        message: `Telegram 请求失败：${description}`,
      };
    }
  }

  private async syncDeleteCollectionNavMessages(args: {
    channelId: bigint;
    collectionName: string;
    nameNormalized?: string | null;
  }) {
    const channel = await this.prisma.channel.findUnique({
      where: { id: args.channelId },
      select: {
        id: true,
        tgChatId: true,
        navReplyMarkup: true,
        defaultBot: {
          select: { tokenEncrypted: true },
        },
      },
    });

    if (!channel) return;

    const state = parseCollectionNavState(channel.navReplyMarkup);
    const targetNormalized = normalizeCollectionName(args.nameNormalized || args.collectionName);
    const matchedKeys = new Set<string>();

    for (const key of [
      ...Object.keys(state.detailMessageIds),
      ...Object.keys(state.detailPageMessageIds),
      args.collectionName,
      args.nameNormalized || '',
    ]) {
      if (key && normalizeCollectionName(key) === targetNormalized) {
        matchedKeys.add(key);
      }
    }

    const remainingNavCollectionCount = await this.prisma.collection.count({
      where: {
        channelId: args.channelId,
        navEnabled: true,
      },
    });

    const messageIds = new Set<number>();
    const nextDetailMessageIds = { ...state.detailMessageIds };
    const nextDetailPageMessageIds = { ...state.detailPageMessageIds };

    if (remainingNavCollectionCount === 0) {
      if (state.indexMessageId) {
        messageIds.add(state.indexMessageId);
      }
      for (const messageId of state.indexPageMessageIds) {
        messageIds.add(messageId);
      }
      for (const messageId of Object.values(state.detailMessageIds)) {
        messageIds.add(messageId);
      }
      for (const pageMessageIds of Object.values(state.detailPageMessageIds)) {
        for (const messageId of pageMessageIds) {
          messageIds.add(messageId);
        }
      }
    } else {
      for (const key of matchedKeys) {
        const firstMessageId = nextDetailMessageIds[key];
        if (firstMessageId) {
          messageIds.add(firstMessageId);
        }
        for (const pageMessageId of nextDetailPageMessageIds[key] ?? []) {
          messageIds.add(pageMessageId);
        }
        delete nextDetailMessageIds[key];
        delete nextDetailPageMessageIds[key];
      }
    }

    const nextState =
      remainingNavCollectionCount === 0
        ? null
        : {
            indexMessageId: state.indexMessageId,
            indexPageMessageIds: state.indexPageMessageIds,
            detailMessageIds: nextDetailMessageIds,
            detailPageMessageIds: nextDetailPageMessageIds,
          };

    await this.prisma.channel.update({
      where: { id: args.channelId },
      data: {
        navReplyMarkup: mergeCollectionNavStateIntoReplyMarkup(channel.navReplyMarkup, nextState) as Prisma.InputJsonValue,
      },
    });

    const botToken = channel.defaultBot?.tokenEncrypted?.trim() || '';
    const chatId = channel.tgChatId?.trim() || '';

    if (!botToken || !chatId || messageIds.size === 0) {
      return;
    }

    for (const messageId of messageIds) {
      try {
        await this.deleteTelegramMessage({
          botToken,
          chatId,
          messageId,
        });
      } catch (error) {
        const deleteError = error as { code?: string; message?: string } | null | undefined;
        if (isTelegramMessageDeleteNotFoundError(deleteError)) {
          continue;
        }
        console.error('[collection] 同步清理 TG 合集目录消息失败（不阻塞）', {
          channelId: args.channelId.toString(),
          collectionName: args.collectionName,
          messageId,
          error: deleteError?.message ?? deleteError,
        });
      }
    }
  }

  private toResponse(row: any) {
    const episodes = Array.isArray(row.episodes) ? row.episodes : [];
    const now = Date.now();

    const blockedEpisode = episodes
      .map((ep: any) => {
        const sourceMeta = ep?.mediaAsset?.sourceMeta && typeof ep.mediaAsset.sourceMeta === 'object'
          ? (ep.mediaAsset.sourceMeta as Record<string, unknown>)
          : {};
        const skipStatus = typeof sourceMeta.skipStatus === 'string' ? sourceMeta.skipStatus : null;
        const isSkippedMissing = skipStatus === 'skipped_missing';
        const dispatchTasks = Array.isArray(ep?.mediaAsset?.dispatchTasks) ? ep.mediaAsset.dispatchTasks : [];
        const hasSuccess = dispatchTasks.some((t: any) => t.status === 'success');
        const blocked = !isSkippedMissing && !hasSuccess;
        return {
          episodeNo: ep.episodeNo as number,
          blocked,
          isSkippedMissing,
          skipReason: typeof sourceMeta.skipReason === 'string' ? sourceMeta.skipReason : null,
          skipAt: typeof sourceMeta.skipAt === 'string' ? sourceMeta.skipAt : null,
          mediaUpdatedAt: ep?.mediaAsset?.updatedAt ? new Date(ep.mediaAsset.updatedAt).getTime() : null,
          mediaAssetId: ep?.mediaAsset?.id ? String(ep.mediaAsset.id) : null,
        };
      })
      .filter((item: { blocked: boolean }) => item.blocked)
      .sort(
        (
          a: { episodeNo: number },
          b: { episodeNo: number },
        ) => a.episodeNo - b.episodeNo,
      )[0];

    const blockState = blockedEpisode ? 'blocked' : 'unblocked';
    const waitingPrevEpisodeNo = blockedEpisode?.episodeNo ?? null;
    const blockedDurationSec = blockedEpisode?.mediaUpdatedAt
      ? Math.max(0, Math.floor((now - blockedEpisode.mediaUpdatedAt) / 1000))
      : null;

    const collectionOrderConfig = parseCollectionOrderConfig(row.extConfig);

    return {
      ...row,
      id: row.id.toString(),
      channelId: row.channelId.toString(),
      createdBy: row.createdBy != null ? row.createdBy.toString() : null,
      channel: row.channel
        ? {
            ...row.channel,
            id: row.channel.id.toString(),
          }
        : undefined,
      _count: row._count,
      blockState,
      blockReason: blockedEpisode ? `等待前序集 ${blockedEpisode.episodeNo}` : null,
      waitingPrevEpisodeNo,
      currentEpisodeNo: null,
      blockingTaskId: blockedEpisode?.mediaAssetId ?? null,
      blockedDurationSec,
      inheritChannelOrderConfig: collectionOrderConfig.inheritChannelOrderConfig,
      collectionDispatchGateEnabled: collectionOrderConfig.collectionDispatchGateEnabled,
      collectionHeadBypassEnabled: collectionOrderConfig.collectionHeadBypassEnabled,
      collectionHeadBypassMinutes: collectionOrderConfig.collectionHeadBypassMinutes,
      collectionGapPolicy: collectionOrderConfig.collectionGapPolicy,
      collectionAllowedGapSize: collectionOrderConfig.collectionAllowedGapSize,
    };
  }

  private async buildCollectionEpisodeCountMap(
    rows: Array<{
      id: bigint;
      channelId: bigint;
      name: string;
      nameNormalized?: string | null;
      _count?: { episodes?: number } | null;
    }>,
  ) {
    const counts = new Map<string, number>();
    if (rows.length === 0) return counts;

    const collectionIdsByChannelName = new Map<string, string>();
    const channelIds = [...new Set(rows.map((row) => row.channelId.toString()))];

    for (const row of rows) {
      const normalizedName = normalizeCollectionName(row.nameNormalized || row.name);
      collectionIdsByChannelName.set(`${row.channelId.toString()}:${normalizedName}`, row.id.toString());
      counts.set(row.id.toString(), row._count?.episodes ?? 0);
    }

    const dispatchTasks = await this.prisma.dispatchTask.findMany({
      where: {
        channelId: { in: channelIds.map((item) => BigInt(item)) },
        status: 'success',
        telegramMessageLink: { not: null },
      },
      select: {
        channelId: true,
        mediaAssetId: true,
        mediaAsset: {
          select: {
            sourceMeta: true,
          },
        },
      },
    });

    const seenMediaAssetIdsByCollection = new Map<string, Set<string>>();

    for (const task of dispatchTasks) {
      const parsed = parseCollectionMeta(task.mediaAsset?.sourceMeta);
      if (!parsed) continue;

      const normalizedName = normalizeCollectionName(parsed.collectionName);
      const collectionId = collectionIdsByChannelName.get(`${task.channelId.toString()}:${normalizedName}`);
      if (!collectionId) continue;

      const mediaAssetId = task.mediaAssetId.toString();
      const seen = seenMediaAssetIdsByCollection.get(collectionId) ?? new Set<string>();
      seen.add(mediaAssetId);
      seenMediaAssetIdsByCollection.set(collectionId, seen);
    }

    for (const [collectionId, seen] of seenMediaAssetIdsByCollection.entries()) {
      counts.set(collectionId, seen.size);
    }

    return counts;
  }

  async list(userId?: string, role?: string) {
    const where: Prisma.CollectionWhereInput =
      role === 'admin'
        ? {}
        : {
            createdBy: userId ? BigInt(userId) : undefined,
          };

    const rows = await this.prisma.collection.findMany({
      where,
      orderBy: { updatedAt: 'desc' },
      include: {
        channel: { select: { id: true, name: true } },
        _count: { select: { episodes: true } },
        episodes: {
          select: {
            episodeNo: true,
            mediaAsset: {
              select: {
                id: true,
                updatedAt: true,
                sourceMeta: true,
                dispatchTasks: {
                  select: { status: true },
                  where: { status: 'success' },
                  take: 1,
                },
              },
            },
          },
        },
      },
    });

    const episodeCountMap = await this.buildCollectionEpisodeCountMap(rows);

    return rows.map((row) =>
      this.toResponse({
        ...row,
        _count: {
          ...(row._count ?? {}),
          episodes: episodeCountMap.get(row.id.toString()) ?? row._count?.episodes ?? 0,
        },
        episodeCount: episodeCountMap.get(row.id.toString()) ?? row._count?.episodes ?? 0,
      }),
    );
  }

  async create(dto: SaveCollectionDto, userId?: string, role?: string) {
    const channel = await this.prisma.channel.findFirst({
      where:
        role === 'admin'
          ? { id: BigInt(dto.channelId) }
          : {
              id: BigInt(dto.channelId),
              createdBy: userId ? BigInt(userId) : undefined,
            },
      select: { id: true, folderPath: true },
    });
    if (!channel) throw new NotFoundException('channel not found');

    const normalizedName = normalizeCollectionName(dto.name || '');
    if (!normalizedName) {
      throw new BadRequestException('name is required');
    }

    const existed = await this.prisma.collection.findFirst({
      where: {
        channelId: BigInt(dto.channelId),
        nameNormalized: normalizedName,
      },
      select: { id: true, name: true },
    });
    if (existed) {
      throw new BadRequestException(`合集名称已存在: ${existed.name}`);
    }

    await this.ensureCollectionFolderExists({
      channelFolderPath: channel.folderPath,
      collectionName: normalizedName,
    });

    const created = await this.prisma.collection.create({
      data: {
        channelId: BigInt(dto.channelId),
        name: normalizedName,
        nameNormalized: normalizedName,
        slug: dto.slug || null,
        dirPath: dto.dirPath,
        extConfig: mergeCollectionOrderConfigIntoExtConfig(undefined, dto),
        status: DEFAULT_COLLECTION_STATUS,
        sortOrder: dto.sortOrder,
        navEnabled: dto.navEnabled,
        navPageSize: dto.navPageSize,
        templateText: dto.templateText || null,
        createdBy: role === 'admin' ? null : userId ? BigInt(userId) : null,
      },
      include: {
        channel: { select: { id: true, name: true } },
        _count: { select: { episodes: true } },
      },
    });

    await this.enqueueSearchIndexJob({
      sourceType: 'collection',
      sourceId: created.id.toString(),
      channelId: created.channelId.toString(),
    });
    await this.enqueueCatalogRefresh(created.channelId.toString());

    return this.toResponse(created);
  }

  async update(id: string, dto: Partial<SaveCollectionDto>, userId?: string, role?: string) {
    const existing = await this.prisma.collection.findFirst({
      where:
        role === 'admin'
          ? { id: BigInt(id) }
          : { id: BigInt(id), createdBy: userId ? BigInt(userId) : undefined },
      select: {
        id: true,
        channelId: true,
        name: true,
        nameNormalized: true,
        navEnabled: true,
        indexMessageId: true,
        indexPageMessageIds: true,
        lastBuiltAt: true,
        extConfig: true,
        channel: { select: { folderPath: true, tgChatId: true } },
        _count: { select: { episodes: true } },
      },
    });
    if (!existing) throw new NotFoundException('collection not found');

    const nextChannelId = dto.channelId ? BigInt(dto.channelId) : existing.channelId;
    const nameProvided = typeof dto.name === 'string';
    const nextNameNormalized = nameProvided ? normalizeCollectionName(dto.name || '') : existing.nameNormalized;

    if (nameProvided && !nextNameNormalized) {
      throw new BadRequestException('name is required');
    }

    const channel = await this.prisma.channel.findFirst({
      where:
        role === 'admin'
          ? { id: nextChannelId }
          : { id: nextChannelId, createdBy: userId ? BigInt(userId) : undefined },
      select: { id: true, folderPath: true, tgChatId: true },
    });

    if (!channel) {
      throw new NotFoundException('channel not found');
    }

    const isNameOrChannelChanged =
      nextChannelId !== existing.channelId || nextNameNormalized !== existing.nameNormalized;

    const isNameChanged = nextNameNormalized !== existing.nameNormalized;
    const hasCatalogRecords = Boolean(
      existing.navEnabled ||
      existing.indexMessageId != null ||
      (Array.isArray(existing.indexPageMessageIds) && existing.indexPageMessageIds.length > 0) ||
      existing.lastBuiltAt != null,
    );

    if (isNameChanged && hasCatalogRecords) {
      throw new BadRequestException('该合集已有目录记录，禁止修改合集名称');
    }

    if (isNameChanged && (existing._count?.episodes ?? 0) > 0) {
      throw new BadRequestException('该合集已有视频，禁止修改合集名称');
    }

    if (isNameOrChannelChanged) {
      const conflict = await this.prisma.collection.findFirst({
        where: {
          id: { not: existing.id },
          channelId: nextChannelId,
          nameNormalized: nextNameNormalized,
        },
        select: { id: true, name: true },
      });

      if (conflict) {
        throw new BadRequestException(`合集名称已存在: ${conflict.name}`);
      }

      await this.ensureCollectionFolderExists({
        channelFolderPath: channel.folderPath,
        collectionName: nextNameNormalized,
      });
    }

    const updated = await this.prisma.collection.update({
      where: { id: BigInt(id) },
      data: {
        channelId: dto.channelId ? BigInt(dto.channelId) : undefined,
        name: nameProvided ? nextNameNormalized : undefined,
        nameNormalized: nameProvided ? nextNameNormalized : undefined,
        slug: dto.slug || undefined,
        dirPath: dto.dirPath,
        extConfig: mergeCollectionOrderConfigIntoExtConfig(existing.extConfig, {
          inheritChannelOrderConfig:
            typeof dto.inheritChannelOrderConfig === 'boolean'
              ? dto.inheritChannelOrderConfig
              : parseCollectionOrderConfig(existing.extConfig).inheritChannelOrderConfig,
          collectionDispatchGateEnabled:
            typeof dto.collectionDispatchGateEnabled === 'boolean'
              ? dto.collectionDispatchGateEnabled
              : parseCollectionOrderConfig(existing.extConfig).collectionDispatchGateEnabled,
          collectionHeadBypassEnabled:
            typeof dto.collectionHeadBypassEnabled === 'boolean'
              ? dto.collectionHeadBypassEnabled
              : parseCollectionOrderConfig(existing.extConfig).collectionHeadBypassEnabled,
          collectionHeadBypassMinutes:
            dto.collectionHeadBypassMinutes ?? parseCollectionOrderConfig(existing.extConfig).collectionHeadBypassMinutes,
          collectionGapPolicy:
            dto.collectionGapPolicy ?? parseCollectionOrderConfig(existing.extConfig).collectionGapPolicy,
          collectionAllowedGapSize:
            dto.collectionAllowedGapSize ?? parseCollectionOrderConfig(existing.extConfig).collectionAllowedGapSize,
        }),
        status: DEFAULT_COLLECTION_STATUS,
        sortOrder: dto.sortOrder,
        navEnabled: dto.navEnabled,
        navPageSize: dto.navPageSize,
        templateText: dto.templateText || undefined,
      },
      include: {
        channel: { select: { id: true, name: true } },
        _count: { select: { episodes: true } },
      },
    });

    await this.enqueueSearchIndexJob({
      sourceType: 'collection',
      sourceId: updated.id.toString(),
      channelId: updated.channelId.toString(),
    });
    await this.enqueueCatalogRefresh(updated.channelId.toString());

    return this.toResponse(updated);
  }

  private formatCollectionEpisodeTitle(args: {
    episodeNo: number;
    episodeTitle?: string | null;
    sourceTitle?: string | null;
    templateText?: string | null;
  }) {
    const safeTitle = (args.episodeTitle || '').trim();
    if (safeTitle) return safeTitle;

    const fallbackTitle = (args.sourceTitle || '').trim() || `第${args.episodeNo}集`;

    const safeTemplate = (args.templateText || '').trim();
    if (safeTemplate) {
      return safeTemplate
        .replace(/\{episodeNo\}/g, String(args.episodeNo))
        .replace(/\{title\}/g, fallbackTitle);
    }

    return fallbackTitle;
  }

  async getCatalogPreview(
    id: string,
    userId?: string,
    role?: string,
    pagination?: { page?: string; pageSize?: string },
  ) {
    const collection = await this.prisma.collection.findFirst({
      where:
        role === 'admin'
          ? { id: BigInt(id) }
          : { id: BigInt(id), createdBy: userId ? BigInt(userId) : undefined },
      select: {
        id: true,
        name: true,
        templateText: true,
        navPageSize: true,
        channelId: true,
        channel: {
          select: {
            name: true,
            tgChatId: true,
          },
        },
      },
    });

    if (!collection) throw new NotFoundException('collection not found');

    const safePage = Math.max(1, Number.parseInt((pagination?.page || '').trim(), 10) || 1);
    const configuredPageSize = Number(collection.navPageSize ?? 20);
    const safePageSize = Math.min(100, Math.max(1, Number.isFinite(configuredPageSize) ? configuredPageSize : 20));

    const dispatchTasks = await this.prisma.dispatchTask.findMany({
      where: {
        channelId: collection.channelId,
        status: 'success',
        telegramMessageLink: { not: null },
      },
      orderBy: { finishedAt: 'desc' },
      select: {
        id: true,
        mediaAssetId: true,
        telegramMessageLink: true,
        mediaAsset: {
          select: {
            originalName: true,
            sourceMeta: true,
          },
        },
      },
    });

    const episodeOverrideRows = await this.prisma.collectionEpisode.findMany({
      where: {
        collectionId: collection.id,
      },
      select: {
        id: true,
        mediaAssetId: true,
        episodeNo: true,
        episodeTitle: true,
      },
    });

    const overrideByMediaAssetId = new Map(
      episodeOverrideRows.map((row) => [row.mediaAssetId.toString(), row]),
    );

    const mappedEpisodes = dispatchTasks
      .map((task) => {
        const sourceMeta = task.mediaAsset?.sourceMeta;
        const parsed = parseCollectionMeta(sourceMeta);
        if (!parsed) return null;
        if (parsed.collectionName !== collection.name) return null;

        const mediaAssetId = task.mediaAssetId.toString();
        const override = overrideByMediaAssetId.get(mediaAssetId);
        const sourceTitle = getFileStem(task.mediaAsset?.originalName || '');

        return {
          id: override?.id?.toString() || `dispatch-${task.id.toString()}`,
          mediaAssetId,
          episodeNo: parsed.episodeNo,
          title: this.formatCollectionEpisodeTitle({
            episodeNo: parsed.episodeNo,
            episodeTitle: override?.episodeTitle,
            sourceTitle,
            templateText: collection.templateText,
          }),
          link: task.telegramMessageLink || '',
          readonlyLink: task.telegramMessageLink || '',
        };
      })
      .filter((item): item is NonNullable<typeof item> => Boolean(item))
      .sort((a, b) => a.episodeNo - b.episodeNo);

    const total = mappedEpisodes.length;
    const skip = (safePage - 1) * safePageSize;
    const paged = mappedEpisodes.slice(skip, skip + safePageSize);

    const videos = paged.map((ep, idx) => ({
      ...ep,
      order: skip + idx + 1,
    }));

    return {
      collectionId: collection.id.toString(),
      collectionName: collection.name,
      channelId: collection.channelId.toString(),
      channelName: collection.channel?.name || '-',
      title: `${collection.name} 合集目录`,
      videos,
      page: safePage,
      pageSize: safePageSize,
      total,
      totalPages: Math.max(1, Math.ceil(total / safePageSize)),
    };
  }

  async updateCatalogTitle(
    id: string,
    body: { episodeId?: string; title?: string },
    userId?: string,
    role?: string,
  ) {
    const episodeId = String(body.episodeId || '').trim();
    const title = String(body.title || '').trim();

    if (!episodeId) {
      throw new BadRequestException('episodeId is required');
    }
    if (!title) {
      throw new BadRequestException('title is required');
    }

    const collection = await this.prisma.collection.findFirst({
      where:
        role === 'admin'
          ? { id: BigInt(id) }
          : { id: BigInt(id), createdBy: userId ? BigInt(userId) : undefined },
      select: { id: true, channelId: true, name: true },
    });
    if (!collection) throw new NotFoundException('collection not found');

    const episode = await this.prisma.collectionEpisode.findFirst({
      where: {
        id: BigInt(episodeId),
        collectionId: collection.id,
      },
      select: {
        id: true,
        mediaAssetId: true,
      },
    });

    if (episode) {
      await this.prisma.collectionEpisode.update({
        where: { id: episode.id },
        data: {
          episodeTitle: title,
        },
      });
    } else {
      const taskIdText = episodeId.startsWith('dispatch-') ? episodeId.slice('dispatch-'.length) : '';
      if (!taskIdText || !/^\d+$/.test(taskIdText)) {
        throw new NotFoundException('collection episode not found');
      }

      const dispatchTask = await this.prisma.dispatchTask.findFirst({
        where: {
          id: BigInt(taskIdText),
          channelId: collection.channelId,
          status: 'success',
          telegramMessageLink: { not: null },
        },
        select: {
          mediaAssetId: true,
          telegramMessageLink: true,
          mediaAsset: {
            select: {
              originalName: true,
              sourceMeta: true,
            },
          },
        },
      });

      if (!dispatchTask) {
        throw new NotFoundException('collection episode not found');
      }

      const parsed = parseCollectionMeta(dispatchTask.mediaAsset?.sourceMeta);
      if (!parsed || parsed.collectionName !== collection.name) {
        throw new NotFoundException('collection episode not found');
      }

      const fileNameSnapshot = getFileStem(dispatchTask.mediaAsset?.originalName || '') || `第${parsed.episodeNo}集`;

      await this.prisma.collectionEpisode.upsert({
        where: {
          mediaAssetId: dispatchTask.mediaAssetId,
        },
        update: {
          collectionId: collection.id,
          episodeNo: parsed.episodeNo,
          fileNameSnapshot,
          episodeTitle: title,
        },
        create: {
          collectionId: collection.id,
          mediaAssetId: dispatchTask.mediaAssetId,
          episodeNo: parsed.episodeNo,
          fileNameSnapshot,
          parseStatus: 'ok',
          sortKey: String(parsed.episodeNo).padStart(6, '0'),
          telegramMessageLink: dispatchTask.telegramMessageLink,
          publishedAt: new Date(),
          episodeTitle: title,
        },
      });
    }

    await this.enqueueSearchIndexJob({
      sourceType: 'collection',
      sourceId: collection.id.toString(),
      channelId: collection.channelId.toString(),
    });

    return { ok: true };
  }

  async remove(id: string, userId?: string, role?: string) {
    const existing = await this.prisma.collection.findFirst({
      where:
        role === 'admin'
          ? { id: BigInt(id) }
          : { id: BigInt(id), createdBy: userId ? BigInt(userId) : undefined },
      select: {
        id: true,
        name: true,
        nameNormalized: true,
        channel: {
          select: { id: true, folderPath: true },
        },
      },
    });
    if (!existing) throw new NotFoundException('collection not found');

    try {
      const episodeRows = await this.prisma.collectionEpisode.findMany({
        where: { collectionId: existing.id },
        select: { mediaAssetId: true },
      });

      const metaAssets = await this.prisma.mediaAsset.findMany({
        where: {
          channelId: existing.channel.id,
          AND: [
            { sourceMeta: { path: ['isCollection'], equals: true } },
            { sourceMeta: { path: ['collectionName'], equals: existing.name } },
          ],
        },
        select: { id: true },
      });

      const mediaAssetIds = Array.from(
        new Set([
          ...episodeRows.map((row) => row.mediaAssetId),
          ...metaAssets.map((row) => row.id),
        ]),
      );

      await this.prisma.$transaction(async (tx) => {
        if (mediaAssetIds.length > 0) {
          await tx.dispatchTask.deleteMany({
            where: { mediaAssetId: { in: mediaAssetIds } },
          });

          await tx.mediaAsset.deleteMany({
            where: { id: { in: mediaAssetIds } },
          });
        }

        await tx.collection.delete({
          where: { id: existing.id },
        });
      });

      await this.removeCollectionFolderIfExists({
        channelFolderPath: existing.channel.folderPath,
        collectionName: existing.name,
      });

      await this.syncDeleteCollectionNavMessages({
        channelId: existing.channel.id,
        collectionName: existing.name,
        nameNormalized: existing.nameNormalized,
      });

      await this.enqueueSearchIndexJob({
        sourceType: 'collection',
        sourceId: existing.id.toString(),
        action: 'delete',
      });
      await this.enqueueCatalogRefresh(existing.channel.id.toString());

      return { ok: true };
    } catch (error) {
      throw new InternalServerErrorException(
        `删除合集失败: ${error instanceof Error ? error.message : 'unknown_error'}`,
      );
    }
  }

  private async enqueueSearchIndexJob(payload: {
    sourceType: string;
    sourceId: string;
    channelId?: string;
    action?: 'upsert' | 'delete';
  }) {
    try {
      const queue = getSearchIndexQueue();
      await queue.add('upsert', payload, {
        jobId: `search-index-${payload.sourceType}-${payload.sourceId}`,
        removeOnComplete: true,
        removeOnFail: 200,
      });
    } catch (error) {
      console.error('[collection] 搜索索引入队失败（不阻塞）', error);
    }
  }

  private async enqueueCatalogRefresh(channelId: string) {
    try {
      const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
      const connection = new IORedis(redisUrl, { maxRetriesPerRequest: null });
      const queue = new Queue('q_catalog', { connection: connection as any });
      await queue.add(
        'catalog-refresh-by-collection-change',
        { channelIdRaw: channelId, source: 'collection-change' },
        {
          jobId: `catalog-refresh-${channelId}`,
          removeOnComplete: true,
          removeOnFail: 100,
        },
      );
      await queue.close();
      await connection.quit();
    } catch (error) {
      console.error('[collection] 目录刷新入队失败（不阻塞）', error);
    }
  }
}

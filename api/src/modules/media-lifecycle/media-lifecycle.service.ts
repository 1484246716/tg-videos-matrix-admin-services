import {
  ConflictException,
  Injectable,
  InternalServerErrorException,
  NotFoundException,
  BadRequestException,
  ForbiddenException,
} from '@nestjs/common';
import { PrismaClientKnownRequestError } from '@prisma/client/runtime/library';
import { access, unlink } from 'node:fs/promises';
import IORedis from 'ioredis';
import { PrismaService } from '../prisma/prisma.service';

let mediaLifecycleRedis: IORedis | null = null;

function getMediaLifecycleRedis() {
  if (mediaLifecycleRedis) return mediaLifecycleRedis;
  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
  mediaLifecycleRedis = new IORedis(redisUrl, { maxRetriesPerRequest: null });
  mediaLifecycleRedis.on('error', (error) => {
    console.error('[redis] media-lifecycle redis error:', error?.message ?? error);
  });
  return mediaLifecycleRedis;
}

const STAGE_FILTER_MAP: Record<string, { mediaStatus?: any; dispatchStatus?: any; catalogStatus?: any }> = {
  scanned: { mediaStatus: 'ready' },
  relay_uploading: { mediaStatus: 'ingesting' },
  relay_uploaded: { mediaStatus: 'relay_uploaded' },
  dispatching: { dispatchStatus: 'running' },
  dispatched: { dispatchStatus: 'success' },
  cataloged: { catalogStatus: 'success' },
  failed: { mediaStatus: 'failed' },
};

const INGEST_STUCK_TIMEOUT_CODE = 'INGEST_STUCK_TIMEOUT';
const INGEST_STUCK_FORCE_DELETE_GRACE_MS = Number(
  process.env.TYPEA_INGEST_STALE_MS || '1800000',
);

type GroupLifecycleStatus = 'running' | 'success' | 'failed' | 'partial_failed';

type GroupDispatchAggregate = {
  total: number;
  success: number;
  running: number;
  failed: number;
};

function resolveGroupLifecycleStatus(args: {
  taskStatus?: string | null;
  aggregate: GroupDispatchAggregate;
}): GroupLifecycleStatus {
  const { total, success, running, failed } = args.aggregate;

  // 优先以子消息派发结果为准：只要子消息全成功，就应展示“组成功”
  if (total > 0 && success >= total) return 'success';
  if (success > 0 && failed > 0) return 'partial_failed';
  if (failed > 0 && success === 0 && running === 0) return 'failed';
  if (running > 0 || (success > 0 && success < total)) return 'running';

  const taskStatus = (args.taskStatus || '').toLowerCase();
  if (taskStatus === 'success') return 'success';
  if (taskStatus === 'failed' || taskStatus === 'dead' || taskStatus === 'cancelled') {
    return success > 0 ? 'partial_failed' : 'failed';
  }

  return 'running';
}

async function removeFileIfExists(filePath?: string | null) {
  if (!filePath) return { deleted: false, reason: 'empty_path' };

  try {
    await access(filePath);
  } catch {
    return { deleted: false, reason: 'not_found' };
  }

  try {
    await unlink(filePath);
    return { deleted: true, reason: 'deleted' };
  } catch (error) {
    return {
      deleted: false,
      reason: error instanceof Error ? error.message : String(error),
    };
  }
}

function normalizeCollectionKey(name: string) {
  return name
    .normalize('NFKC')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseCollectionMeta(sourceMeta: unknown) {
  if (!sourceMeta || typeof sourceMeta !== 'object') return null;
  const meta = sourceMeta as Record<string, unknown>;
  if (meta.isCollection !== true) return null;

  const collectionName = typeof meta.collectionName === 'string' ? meta.collectionName : '';
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


@Injectable()
export class MediaLifecycleService {
  constructor(private readonly prisma: PrismaService) { }

  async list(params: {
    channelId?: string;
    telegramFileId?: string;
    keyword?: string;
    stage?: string;
    mediaType?: string;
    limit?: number;
    page?: number;
    pageSize?: number;
    userId?: string;
    role?: string;
  }) {
    const stageFilter = params.stage ? STAGE_FILTER_MAP[params.stage] : undefined;
    const tfid = (params.telegramFileId || '').trim();
    const normalizedMediaType = (params.mediaType || '').trim().toLowerCase();

    const where = {
      channelId: params.channelId ? BigInt(params.channelId) : undefined,
      ...(tfid
        ? {
          OR: [
            {
              telegramFileId: {
                contains: tfid,
                mode: 'insensitive' as const,
              },
            },
            {
              telegramFileUniqueId: {
                contains: tfid,
                mode: 'insensitive' as const,
              },
            },
          ],
        }
        : {}),
      ...(normalizedMediaType === 'collection'
        ? {
          sourceMeta: {
            path: ['isCollection'],
            equals: true,
          },
        }
        : {}),
      originalName: params.keyword ? { contains: params.keyword, mode: 'insensitive' as const } : undefined,
      status: stageFilter?.mediaStatus ? stageFilter.mediaStatus : undefined,
      channel:
        params.role === 'admin'
          ? undefined
          : {
            createdBy: params.userId ? BigInt(params.userId) : undefined,
          },
    };

    const usePagination = Number.isFinite(params.page) || Number.isFinite(params.pageSize);
    const pageSize = Math.max(1, Math.min(200, Math.floor(params.pageSize ?? params.limit ?? 50)));
    const page = Math.max(1, Math.floor(params.page ?? 1));
    const skip = (page - 1) * pageSize;

    const [total, mediaAssets] = await this.prisma.$transaction([
      this.prisma.mediaAsset.count({ where }),
      this.prisma.mediaAsset.findMany({
        where,
        orderBy: { updatedAt: 'desc' },
        ...(usePagination ? { skip, take: pageSize } : { take: params.limit ?? 50 }),
        include: {
          channel: { select: { id: true, name: true, createdBy: true } },
        },
      }),
    ]);

    const mediaIds = mediaAssets.map((asset) => asset.id);

    const dispatchTasks = mediaIds.length
      ? await this.prisma.dispatchTask.findMany({
        where: {
          mediaAssetId: { in: mediaIds },
          ...(stageFilter?.dispatchStatus ? { status: stageFilter.dispatchStatus } : {}),
        },
        orderBy: { createdAt: 'desc' },
        include: { channel: { select: { id: true, name: true } } },
      })
      : [];

    const groupKeySet = new Set<string>();
    const groupKeyChannelMap = new Map<string, bigint>();
    for (const task of dispatchTasks) {
      if (!task.groupKey) continue;
      const composite = `${task.channelId.toString()}:${task.groupKey}`;
      groupKeySet.add(composite);
      if (!groupKeyChannelMap.has(composite)) {
        groupKeyChannelMap.set(composite, task.channelId);
      }
    }

    const groupTaskWhere = Array.from(groupKeySet).map((composite) => {
      const splitAt = composite.indexOf(':');
      const groupKey = splitAt >= 0 ? composite.slice(splitAt + 1) : composite;
      return {
        channelId: groupKeyChannelMap.get(composite)!,
        groupKey,
      };
    });

    const dispatchGroupTasks = groupTaskWhere.length
      ? await this.prisma.dispatchGroupTask.findMany({
        where: {
          OR: groupTaskWhere,
        },
        select: {
          channelId: true,
          groupKey: true,
          status: true,
          expectedMediaCount: true,
          actualReadyCount: true,
          actualUploadedCount: true,
        },
      })
      : [];

    const dispatchGroupTaskMap = new Map<string, (typeof dispatchGroupTasks)[number]>();
    for (const groupTask of dispatchGroupTasks) {
      dispatchGroupTaskMap.set(`${groupTask.channelId.toString()}:${groupTask.groupKey}`, groupTask);
    }

    const dispatchMap = new Map<string, typeof dispatchTasks>();
    dispatchTasks.forEach((task) => {
      const key = task.mediaAssetId.toString();
      const current = dispatchMap.get(key) ?? [];
      current.push(task);
      dispatchMap.set(key, current);
    });

    const list = mediaAssets.map((asset) => {
      const dispatches = dispatchMap.get(asset.id.toString()) ?? [];
      const latestDispatch = dispatches[0];

      const sourceMeta =
        asset.sourceMeta && typeof asset.sourceMeta === 'object'
          ? (asset.sourceMeta as Record<string, unknown>)
          : {};
      const isCollection = sourceMeta.isCollection === true;

      const groupKey = latestDispatch?.groupKey || null;
      let groupStatus: GroupLifecycleStatus | null = null;
      let groupProgress: { success: number; arrived: number; total: number } | null = null;

      if (groupKey) {
        const groupDispatches = dispatchTasks.filter(
          (task) => task.channelId === asset.channelId && task.groupKey === groupKey,
        );

        const aggregate: GroupDispatchAggregate = groupDispatches.reduce(
          (acc, task) => {
            acc.total += 1;
            if (task.status === 'success') {
              acc.success += 1;
            } else if (task.status === 'failed' || task.status === 'dead' || task.status === 'cancelled') {
              acc.failed += 1;
            } else {
              acc.running += 1;
            }
            return acc;
          },
          { total: 0, success: 0, running: 0, failed: 0 },
        );

        const groupTask = dispatchGroupTaskMap.get(`${asset.channelId.toString()}:${groupKey}`) ?? null;
        groupStatus = resolveGroupLifecycleStatus({
          taskStatus: groupTask?.status ?? null,
          aggregate,
        });

        const expected = Number(groupTask?.expectedMediaCount ?? 0);
        const total = expected > 0 ? expected : aggregate.total;
        const arrived = Number(groupTask?.actualReadyCount ?? 0) > 0 ? Number(groupTask?.actualReadyCount ?? 0) : aggregate.total;
        groupProgress = {
          success: aggregate.success,
          arrived,
          total,
        };
      }

      return {
        id: asset.id.toString(),
        originalName: asset.originalName,
        channelId: asset.channelId.toString(),
        channelName: (asset as any).channel?.name ?? null,
        channelCreatedBy: (asset as any).channel?.createdBy ? (asset as any).channel.createdBy.toString() : null,
        status: asset.status,
        relayMessageId: asset.relayMessageId ? asset.relayMessageId.toString() : null,
        telegramFileId: asset.telegramFileId,
        ingestError: asset.ingestError,
        fileSize: asset.fileSize ? asset.fileSize.toString() : null,
        createdAt: asset.createdAt,
        updatedAt: asset.updatedAt,
        ingestStartedAt: asset.ingestStartedAt,
        ingestFinishedAt: asset.ingestFinishedAt,
        ingestDurationSec: asset.ingestDurationSec,
        latestDispatch: latestDispatch
          ? {
            id: latestDispatch.id.toString(),
            status: latestDispatch.status,
            channelName: (latestDispatch as any).channel?.name ?? null,
            telegramMessageLink: latestDispatch.telegramMessageLink,
            telegramErrorMessage: latestDispatch.telegramErrorMessage,
            groupKey,
            groupStatus,
            groupProgress,
          }
          : null,
        mediaType: isCollection ? 'collection' : 'normal',
        collectionName:
          isCollection && typeof sourceMeta.collectionName === 'string'
            ? sourceMeta.collectionName
            : null,
        episodeNo:
          isCollection &&
            (typeof sourceMeta.episodeNo === 'number' ||
              (typeof sourceMeta.episodeNo === 'string' && /^\d+$/.test(sourceMeta.episodeNo)))
            ? Number(sourceMeta.episodeNo)
            : null,
        ingestErrorCode:
          (() => {
            const code = sourceMeta.ingestErrorCode;
            return typeof code === 'string' ? code : null;
          })(),
        skipStatus:
          (() => {
            const value = sourceMeta.skipStatus;
            return typeof value === 'string' ? value : null;
          })(),
        skipReason:
          (() => {
            const value = sourceMeta.skipReason;
            return typeof value === 'string' ? value : null;
          })(),
        skipAt:
          (() => {
            const value = sourceMeta.skipAt;
            return typeof value === 'string' ? value : null;
          })(),
        blockState:
          (() => {
            if (!isCollection) return null;
            const skipStatus = typeof sourceMeta.skipStatus === 'string' ? sourceMeta.skipStatus : null;
            if (skipStatus === 'skipped_missing') return 'unblocked';
            if (asset.status === 'failed' || asset.status === 'deleted') {
              const elapsedMs = Date.now() - new Date(asset.updatedAt).getTime();
              if (Number.isFinite(elapsedMs) && elapsedMs < 5 * 60 * 1000) return 'waiting';
              return 'blocked';
            }
            return null;
          })(),
        canForceDelete:
          asset.status === 'ingesting' &&
          (() => {
            const sourceMeta =
              asset.sourceMeta && typeof asset.sourceMeta === 'object'
                ? (asset.sourceMeta as Record<string, unknown>)
                : {};
            const ingestErrorCode =
              typeof sourceMeta.ingestErrorCode === 'string'
                ? sourceMeta.ingestErrorCode
                : '';
            if (ingestErrorCode === INGEST_STUCK_TIMEOUT_CODE) return true;

            const updatedAtMs = new Date(asset.updatedAt).getTime();
            if (!updatedAtMs || Number.isNaN(updatedAtMs)) return false;
            return Date.now() - updatedAtMs >= INGEST_STUCK_FORCE_DELETE_GRACE_MS;
          })(),
      };
    });

    if (!usePagination) {
      return list;
    }

    return {
      list,
      pagination: {
        page,
        pageSize,
        total,
        totalPages: Math.max(1, Math.ceil(total / pageSize)),
      },
    };
  }

  async getProgress(ids: string[]) {
    if (!ids.length) return {};
    const redis = getMediaLifecycleRedis();
    const keys = ids.map((id) => `media:progress:${id}`);
    const values = await redis.mget(...keys);
    const result: Record<string, any> = {};

    values.forEach((value, index) => {
      if (!value) return;
      try {
        result[ids[index]] = JSON.parse(value);
      } catch {
        // ignore malformed entries
      }
    });

    return result;
  }

  async getDetail(id: string, userId?: string, role?: string) {
    const asset = await this.prisma.mediaAsset.findFirst({
      where: {
        id: BigInt(id),
        channel:
          role === 'admin'
            ? undefined
            : {
              createdBy: userId ? BigInt(userId) : undefined,
            },
      },
      include: { channel: { select: { id: true, name: true } } },
    });

    if (!asset) throw new NotFoundException('media asset not found');

    const dispatchTasks = await this.prisma.dispatchTask.findMany({
      where: { mediaAssetId: asset.id },
      orderBy: { createdAt: 'desc' },
      include: { channel: { select: { id: true, name: true } } },
    });

    const successDispatch = dispatchTasks.find((t) => t.status === 'success' && t.finishedAt);
    let catalogTasks: any[] = [];

    if (successDispatch && successDispatch.finishedAt) {
      const task = await this.prisma.catalogTask.findFirst({
        where: {
          channelId: asset.channelId,
          createdAt: { gte: successDispatch.finishedAt },
        },
        orderBy: { createdAt: 'desc' },
        include: { channel: { select: { id: true, name: true } } },
      });
      if (task) {
        catalogTasks = [task];
      }
    }

    return {
      mediaAsset: {
        id: asset.id.toString(),
        originalName: asset.originalName,
        channelId: asset.channelId.toString(),
        channelName: asset.channel?.name ?? null,
        status: asset.status,
        relayMessageId: asset.relayMessageId ? asset.relayMessageId.toString() : null,
        telegramFileId: asset.telegramFileId,
        ingestError: asset.ingestError,
        fileSize: asset.fileSize ? asset.fileSize.toString() : null,
        createdAt: asset.createdAt,
        updatedAt: asset.updatedAt,
      },
      dispatchTasks: dispatchTasks.map((task) => ({
        id: task.id.toString(),
        status: task.status,
        channelName: task.channel?.name ?? null,
        telegramMessageLink: task.telegramMessageLink,
        telegramErrorMessage: task.telegramErrorMessage,
        retryCount: task.retryCount,
        finishedAt: task.finishedAt,
      })),
      catalogTasks: catalogTasks.map((task) => ({
        id: task.id.toString(),
        status: task.status,
        channelName: task.channel?.name ?? null,
        telegramMessageLink: task.telegramMessageLink,
        errorMessage: task.errorMessage,
        finishedAt: task.finishedAt,
      })),
    };
  }

  private isCooldownSatisfied(failedAt?: Date | null) {
    if (!failedAt) return true;
    const cooldownMs = 30 * 60 * 1000;
    return Date.now() - failedAt.getTime() >= cooldownMs;
  }

  async retryRelay(id: string, userId?: string, role?: string) {
    const asset = await this.prisma.mediaAsset.findFirst({
      where: {
        id: BigInt(id),
        channel:
          role === 'admin'
            ? undefined
            : {
              createdBy: userId ? BigInt(userId) : undefined,
            },
      },
      select: { id: true, status: true, updatedAt: true, ingestError: true },
    });

    if (!asset) throw new NotFoundException('media asset not found');

    if (asset.status !== 'failed') {
      return { ok: false, reason: 'only_failed_can_retry' };
    }

    if (role !== 'admin' && !this.isCooldownSatisfied(asset.updatedAt)) {
      return { ok: false, reason: 'cooldown_not_reached' };
    }

    await this.prisma.mediaAsset.update({
      where: { id: asset.id },
      data: {
        status: 'ready',
        ingestError: null,
        relayMessageId: null,
        telegramFileId: null,
        updatedAt: new Date(),
      },
    });

    return { ok: true };
  }

  async retryRelayBatch(ids: string[], userId?: string, role?: string) {
    if (!ids.length) return { ok: true, updated: 0, skipped: 0, skippedIds: [] as string[] };

    const assets = await this.prisma.mediaAsset.findMany({
      where: {
        id: { in: ids.map((id) => BigInt(id)) },
        channel:
          role === 'admin'
            ? undefined
            : {
              createdBy: userId ? BigInt(userId) : undefined,
            },
      },
      select: { id: true, status: true, updatedAt: true },
    });

    const eligibleIds: bigint[] = [];
    const skippedIds: string[] = [];

    for (const asset of assets) {
      if (asset.status !== 'failed') {
        skippedIds.push(asset.id.toString());
        continue;
      }
      if (role !== 'admin' && !this.isCooldownSatisfied(asset.updatedAt)) {
        skippedIds.push(asset.id.toString());
        continue;
      }
      eligibleIds.push(asset.id);
    }

    if (eligibleIds.length) {
      await this.prisma.mediaAsset.updateMany({
        where: { id: { in: eligibleIds } },
        data: {
          status: 'ready',
          ingestError: null,
          relayMessageId: null,
          telegramFileId: null,
          updatedAt: new Date(),
        },
      });
    }

    return {
      ok: true,
      updated: eligibleIds.length,
      skipped: skippedIds.length,
      skippedIds,
    };
  }

  async markSkipMissing(id: string, userId?: string, role?: string) {
    if (role !== 'admin') {
      throw new ForbiddenException('仅管理员可执行该操作');
    }

    const asset = await this.prisma.mediaAsset.findFirst({
      where: {
        id: BigInt(id),
        channel:
          role === 'admin'
            ? undefined
            : {
              createdBy: userId ? BigInt(userId) : undefined,
            },
      },
      select: {
        id: true,
        sourceMeta: true,
      },
    });

    if (!asset) throw new NotFoundException('media asset not found');

    const sourceMeta =
      asset.sourceMeta && typeof asset.sourceMeta === 'object'
        ? (asset.sourceMeta as Record<string, unknown>)
        : {};

    if (sourceMeta.isCollection !== true) {
      throw new BadRequestException('仅支持合集资源设置暂缺');
    }

    await this.prisma.mediaAsset.update({
      where: { id: asset.id },
      data: {
        sourceMeta: {
          ...sourceMeta,
          skipStatus: 'skipped_missing',
          skipReason: 'manual_skip_missing',
          skipAt: new Date().toISOString(),
        },
      },
    });

    return { ok: true };
  }

  async revokeSkipMissing(id: string, userId?: string, role?: string) {
    if (role !== 'admin') {
      throw new ForbiddenException('仅管理员可执行该操作');
    }

    const asset = await this.prisma.mediaAsset.findFirst({
      where: {
        id: BigInt(id),
        channel:
          role === 'admin'
            ? undefined
            : {
              createdBy: userId ? BigInt(userId) : undefined,
            },
      },
      select: {
        id: true,
        sourceMeta: true,
      },
    });

    if (!asset) throw new NotFoundException('media asset not found');

    const sourceMeta =
      asset.sourceMeta && typeof asset.sourceMeta === 'object'
        ? (asset.sourceMeta as Record<string, unknown>)
        : {};

    const nextMeta = { ...sourceMeta };
    delete (nextMeta as Record<string, unknown>).skipStatus;
    delete (nextMeta as Record<string, unknown>).skipReason;
    delete (nextMeta as Record<string, unknown>).skipAt;

    await this.prisma.mediaAsset.update({
      where: { id: asset.id },
      data: {
        sourceMeta: nextMeta as any,
      },
    });

    return { ok: true };
  }

  async remove(id: string, force = false, userId?: string, role?: string, groupKey?: string) {
    const mediaAssetId = BigInt(id);
    const safeGroupKey = (groupKey || '').trim();

    const existing = await this.prisma.mediaAsset.findFirst({
      where: {
        id: mediaAssetId,
        channel:
          role === 'admin'
            ? undefined
            : {
              createdBy: userId ? BigInt(userId) : undefined,
            },
      },
      select: {
        id: true,
        channelId: true,
        status: true,
        updatedAt: true,
        sourceMeta: true,
        localPath: true,
        archivePath: true,
      },
    });

    if (!existing) {
      throw new NotFoundException('media asset not found');
    }

    const linkedDispatchTasks = await this.prisma.dispatchTask.findMany({
      where: {
        channelId: existing.channelId,
        ...(safeGroupKey
          ? { groupKey: safeGroupKey }
          : { mediaAssetId: existing.id }),
      },
      select: {
        id: true,
        mediaAssetId: true,
      },
    });

    const targetMediaAssetIdSet = new Set<string>([existing.id.toString()]);
    for (const task of linkedDispatchTasks) {
      targetMediaAssetIdSet.add(task.mediaAssetId.toString());
    }

    const targetMediaAssetIds = [...targetMediaAssetIdSet].map((rawId) => BigInt(rawId));

    const targetAssets = await this.prisma.mediaAsset.findMany({
      where: { id: { in: targetMediaAssetIds } },
      select: {
        id: true,
        status: true,
        updatedAt: true,
        sourceMeta: true,
        localPath: true,
        archivePath: true,
      },
    });

    const snapshotDeleteKeys = new Map<string, { collectionNameNormalized: string; episodeNo: number }>();
    for (const asset of targetAssets) {
      const meta = parseCollectionMeta(asset.sourceMeta);
      if (!meta) continue;
      const collectionNameNormalized = normalizeCollectionKey(meta.collectionName);
      const dedupKey = `${collectionNameNormalized}#${meta.episodeNo}`;
      snapshotDeleteKeys.set(dedupKey, {
        collectionNameNormalized,
        episodeNo: meta.episodeNo,
      });
    }

    const forceNeededAsset = targetAssets.find((asset) => asset.status === 'ingesting');
    if (forceNeededAsset && !force) {
      const sourceMeta =
        forceNeededAsset.sourceMeta && typeof forceNeededAsset.sourceMeta === 'object'
          ? (forceNeededAsset.sourceMeta as Record<string, unknown>)
          : {};
      const ingestErrorCode =
        typeof sourceMeta.ingestErrorCode === 'string'
          ? sourceMeta.ingestErrorCode
          : '';
      const updatedAtMs = new Date(forceNeededAsset.updatedAt).getTime();
      const isLikelyStuck =
        ingestErrorCode === INGEST_STUCK_TIMEOUT_CODE ||
        (!!updatedAtMs && !Number.isNaN(updatedAtMs)
          ? Date.now() - updatedAtMs >= INGEST_STUCK_FORCE_DELETE_GRACE_MS
          : false);

      if (!isLikelyStuck) {
        throw new BadRequestException('该视频正在中转上传中，暂不允许删除');
      }

      throw new BadRequestException(
        '该视频疑似卡住，请使用 force=1 进行强制停止并删除',
      );
    }

    try {
      const fileDeleteResults = await Promise.all(
        targetAssets.map(async (asset) => {
          const localDelete = await removeFileIfExists(asset.localPath);
          const archiveDelete =
            asset.archivePath && asset.archivePath !== asset.localPath
              ? await removeFileIfExists(asset.archivePath)
              : { deleted: false, reason: 'same_or_empty_path' };
          return {
            mediaAssetId: asset.id.toString(),
            local: localDelete,
            archive: archiveDelete,
          };
        }),
      );

      const hasPhysicalDeleteError = fileDeleteResults.some((item) => {
        return [item.local, item.archive].some(
          (result) => result.reason !== 'deleted' && result.reason !== 'not_found' && result.reason !== 'same_or_empty_path',
        );
      });

      if (hasPhysicalDeleteError) {
        throw new InternalServerErrorException('物理文件删除失败，请检查文件权限后重试');
      }

      const deleted = await this.prisma.$transaction(async (tx) => {
        const dispatchTasks = await tx.dispatchTask.findMany({
          where: {
            channelId: existing.channelId,
            ...(safeGroupKey
              ? { groupKey: safeGroupKey }
              : { mediaAssetId: { in: targetMediaAssetIds } }),
          },
          select: { id: true, mediaAssetId: true, telegramMessageId: true },
        });

        const dispatchTaskIds = dispatchTasks.map((task) => task.id);

        if (dispatchTaskIds.length > 0) {
          const sourceDispatchTaskIds = dispatchTasks.map((task) => task.id);
          const messageIds = dispatchTasks
            .map((task) => task.telegramMessageId)
            .filter((value): value is bigint => Boolean(value));

          await tx.riskEvent.deleteMany({
            where: { dispatchTaskId: { in: dispatchTaskIds } },
          });

          await tx.dispatchTaskLog.deleteMany({
            where: { dispatchTaskId: { in: dispatchTaskIds } },
          });

          await (tx as any).catalogSourceItem.deleteMany({
            where: {
              channelId: existing.channelId,
              OR: [
                { sourceDispatchTaskId: { in: sourceDispatchTaskIds } },
                ...(safeGroupKey ? [{ groupKey: safeGroupKey }] : []),
                ...(messageIds.length > 0 ? [{ telegramMessageId: { in: messageIds } }] : []),
              ],
            },
          });

          await tx.dispatchTask.deleteMany({
            where: { id: { in: dispatchTaskIds } },
          });

          if (safeGroupKey) {
            await tx.dispatchGroupTask.deleteMany({
              where: {
                channelId: existing.channelId,
                groupKey: safeGroupKey,
              },
            });
          }
        }

        await tx.mediaAsset.deleteMany({
          where: { id: { in: targetMediaAssetIds } },
        });

        const affectedCollections = new Set<string>();
        for (const snapshotKey of snapshotDeleteKeys.values()) {
          await tx.collectionEpisodeSnapshot.deleteMany({
            where: {
              channelId: existing.channelId,
              collectionNameNormalized: snapshotKey.collectionNameNormalized,
              episodeNo: snapshotKey.episodeNo,
            },
          });
          affectedCollections.add(snapshotKey.collectionNameNormalized);
        }

        for (const collectionNameNormalized of affectedCollections) {
          const episodes = await tx.collectionEpisodeSnapshot.findMany({
            where: {
              channelId: existing.channelId,
              collectionNameNormalized,
            },
            select: {
              episodeNo: true,
              sourceUpdatedAt: true,
            },
            orderBy: { episodeNo: 'asc' },
          });

          if (episodes.length === 0) {
            await tx.collectionSnapshot.upsert({
              where: {
                channelId_collectionNameNormalized: {
                  channelId: existing.channelId,
                  collectionNameNormalized,
                },
              },
              create: {
                channelId: existing.channelId,
                collectionName: collectionNameNormalized,
                collectionNameNormalized,
                episodeCount: 0,
                minEpisodeNo: null,
                maxEpisodeNo: null,
                lastSourceUpdatedAt: null,
                lastRebuildAt: new Date(),
                version: BigInt(1),
                isDeleted: true,
              },
              update: {
                collectionName: collectionNameNormalized,
                episodeCount: 0,
                minEpisodeNo: null,
                maxEpisodeNo: null,
                lastSourceUpdatedAt: null,
                lastRebuildAt: new Date(),
                version: { increment: BigInt(1) },
                isDeleted: true,
              },
            });
            continue;
          }

          const collectionConfig = await tx.collection.findFirst({
            where: { channelId: existing.channelId, nameNormalized: collectionNameNormalized },
            select: { name: true },
          });

          const minEpisodeNo = episodes[0].episodeNo;
          const maxEpisodeNo = episodes[episodes.length - 1].episodeNo;
          const lastSourceUpdatedAt = episodes
            .map((item) => item.sourceUpdatedAt)
            .filter((value): value is Date => Boolean(value))
            .sort((a, b) => b.getTime() - a.getTime())[0] ?? null;

          await tx.collectionSnapshot.upsert({
            where: {
              channelId_collectionNameNormalized: {
                channelId: existing.channelId,
                collectionNameNormalized,
              },
            },
            create: {
              channelId: existing.channelId,
              collectionName: collectionConfig?.name ?? collectionNameNormalized,
              collectionNameNormalized,
              episodeCount: episodes.length,
              minEpisodeNo,
              maxEpisodeNo,
              lastSourceUpdatedAt,
              lastRebuildAt: new Date(),
              version: BigInt(1),
              isDeleted: false,
            },
            update: {
              collectionName: collectionConfig?.name ?? collectionNameNormalized,
              episodeCount: episodes.length,
              minEpisodeNo,
              maxEpisodeNo,
              lastSourceUpdatedAt,
              lastRebuildAt: new Date(),
              version: { increment: BigInt(1) },
              isDeleted: false,
            },
          });
        }

        return {
          deletedMediaAssetIds: targetMediaAssetIds.map((assetId) => assetId.toString()),
          deletedDispatchTaskCount: dispatchTaskIds.length,
          deletedSnapshotCount: snapshotDeleteKeys.size,
        };
      });

      return {
        id: existing.id.toString(),
        ok: true,
        deletedMediaAssetIds: deleted.deletedMediaAssetIds,
        deletedDispatchTaskCount: deleted.deletedDispatchTaskCount,
        physicalDeleted: fileDeleteResults,
      };
    } catch (error) {
      if (
        error instanceof PrismaClientKnownRequestError &&
        error.code === 'P2003'
      ) {
        throw new ConflictException('该视频仍有关联数据，请先处理后再删除');
      }

      throw new InternalServerErrorException('删除失败，关联数据清理异常');
    }
  }
}

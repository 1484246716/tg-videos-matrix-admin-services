import {
  ConflictException,
  Injectable,
  InternalServerErrorException,
  NotFoundException,
} from '@nestjs/common';
import { PrismaClientKnownRequestError } from '@prisma/client/runtime/library';
import IORedis from 'ioredis';
import { PrismaService } from '../prisma/prisma.service';

let mediaLifecycleRedis: IORedis | null = null;

function getMediaLifecycleRedis() {
  if (mediaLifecycleRedis) return mediaLifecycleRedis;
  const redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
  mediaLifecycleRedis = new IORedis(redisUrl, { maxRetriesPerRequest: null });
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

@Injectable()
export class MediaLifecycleService {
  constructor(private readonly prisma: PrismaService) {}

  async list(params: {
    channelId?: string;
    telegramFileId?: string;
    keyword?: string;
    stage?: string;
    limit?: number;
  }) {
    const stageFilter = params.stage ? STAGE_FILTER_MAP[params.stage] : undefined;
    const tfid = (params.telegramFileId || '').trim();

    const mediaAssets = await this.prisma.mediaAsset.findMany({
      where: {
        channelId: params.channelId ? BigInt(params.channelId) : undefined,
        telegramFileId: tfid
          ? {
              contains: tfid,
              mode: 'insensitive',
            }
          : undefined,
        originalName: params.keyword ? { contains: params.keyword, mode: 'insensitive' } : undefined,
        status: stageFilter?.mediaStatus ? stageFilter.mediaStatus : undefined,
      },
      orderBy: { updatedAt: 'desc' },
      take: params.limit ?? 50,
      include: {
        channel: { select: { id: true, name: true } },
      },
    });

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

    const catalogTasks = stageFilter?.catalogStatus
      ? await this.prisma.catalogTask.findMany({
        where: {
          status: stageFilter.catalogStatus,
        },
        orderBy: { createdAt: 'desc' },
        include: { channel: { select: { id: true, name: true } } },
      })
      : [];

    const dispatchMap = new Map<string, typeof dispatchTasks>();
    dispatchTasks.forEach((task) => {
      const key = task.mediaAssetId.toString();
      const current = dispatchMap.get(key) ?? [];
      current.push(task);
      dispatchMap.set(key, current);
    });

    return mediaAssets.map((asset) => {
      const dispatches = dispatchMap.get(asset.id.toString()) ?? [];
      const latestDispatch = dispatches[0];

      return {
        id: asset.id.toString(),
        originalName: asset.originalName,
        channelId: asset.channelId.toString(),
        channelName: (asset as any).channel?.name ?? null,
        status: asset.status,
        relayMessageId: asset.relayMessageId ? asset.relayMessageId.toString() : null,
        telegramFileId: asset.telegramFileId,
        ingestError: asset.ingestError,
        fileSize: asset.fileSize ? asset.fileSize.toString() : null,
        createdAt: asset.createdAt,
        updatedAt: asset.updatedAt,
        latestDispatch: latestDispatch
          ? {
            id: latestDispatch.id.toString(),
            status: latestDispatch.status,
            channelName: (latestDispatch as any).channel?.name ?? null,
            telegramMessageLink: latestDispatch.telegramMessageLink,
            telegramErrorMessage: latestDispatch.telegramErrorMessage,
          }
          : null,
      };
    });
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

  async getDetail(id: string) {
    const asset = await this.prisma.mediaAsset.findUnique({
      where: { id: BigInt(id) },
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

  async retryRelay(id: string) {
    const asset = await this.prisma.mediaAsset.findUnique({
      where: { id: BigInt(id) },
      select: { id: true, status: true, updatedAt: true, ingestError: true },
    });

    if (!asset) throw new NotFoundException('media asset not found');

    if (asset.status !== 'failed') {
      return { ok: false, reason: 'only_failed_can_retry' };
    }

    if (!this.isCooldownSatisfied(asset.updatedAt)) {
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

  async retryRelayBatch(ids: string[]) {
    if (!ids.length) return { ok: true, updated: 0, skipped: 0, skippedIds: [] as string[] };

    const assets = await this.prisma.mediaAsset.findMany({
      where: {
        id: { in: ids.map((id) => BigInt(id)) },
      },
      select: { id: true, status: true, updatedAt: true },
    });

    const eligibleIds: bigint[] = [];
    const skippedIds: string[] = [];

    for (const asset of assets) {
      if (asset.status !== 'failed' || !this.isCooldownSatisfied(asset.updatedAt)) {
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

  async remove(id: string) {
    const mediaAssetId = BigInt(id);

    const existing = await this.prisma.mediaAsset.findUnique({
      where: { id: mediaAssetId },
      select: { id: true },
    });

    if (!existing) {
      throw new NotFoundException('media asset not found');
    }

    try {
      const deleted = await this.prisma.$transaction(async (tx) => {
        const dispatchTasks = await tx.dispatchTask.findMany({
          where: { mediaAssetId },
          select: { id: true },
        });

        const dispatchTaskIds = dispatchTasks.map((task) => task.id);

        if (dispatchTaskIds.length > 0) {
          await tx.riskEvent.deleteMany({
            where: { dispatchTaskId: { in: dispatchTaskIds } },
          });

          await tx.dispatchTaskLog.deleteMany({
            where: { dispatchTaskId: { in: dispatchTaskIds } },
          });

          await tx.dispatchTask.deleteMany({
            where: { id: { in: dispatchTaskIds } },
          });
        }

        return tx.mediaAsset.delete({
          where: { id: mediaAssetId },
        });
      });

      return {
        id: deleted.id.toString(),
        ok: true,
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

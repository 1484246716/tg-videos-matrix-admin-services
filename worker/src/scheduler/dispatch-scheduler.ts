/**
 * Dispatch Scheduler：扫描并入队到期分发任务，并维护分组派发门禁。
 * 在 bootstrap 定时触发，负责顺序闸门判断、频道窗口控制与投递 dispatch worker。
 */

import { MediaStatus, TaskStatus } from '@prisma/client';
import {
  COLLECTION_AUTO_BYPASS_AFTER_MINUTES,
  COLLECTION_AUTO_BYPASS_ENABLED,
  COLLECTION_AUTO_BYPASS_MARK,
  COLLECTION_AUTO_BYPASS_ON_MAX_RETRIES,
  DISPATCH_CHANNEL_INTERVAL_GUARD_ENABLED,
  MAX_SCHEDULE_BATCH,
  TYPEB_GROUP_READY_TIMEOUT_MS,
  TYPEB_GROUP_HARD_DEADLINE_MS,
  TYPEB_GROUP_RETRY_CHECK_MS,
  TYPEB_GROUP_SEAL_QUIET_PERIOD_MS,
  TYPEB_GROUP_SEND_ENABLED,
  TYPEB_GROUP_SEND_MIN_MEDIA_COUNT,
  TYPEB_GROUP_SEND_WHITELIST_CHANNEL_IDS,
} from '../config/env';
import { prisma, getTaskDefinitionModel, withPrismaRetry } from '../infra/prisma';
import { dispatchQueue } from '../infra/redis';
import { logger } from '../logger';
import { catalogSourceWriteMetrics } from '../shared/metrics';
import { releaseChannelLock, tryAcquireChannelLock } from '../shared/channel-lock';
import { updateTaskDefinitionRunStatus } from '../services/task-definition.service';

/**
 * dispatch-scheduler
 *
 * 职责：
 * 1) 扫描可派发的 dispatchTask（pending/scheduled/failed 且到期），决定入队到 q_dispatch。
 * 2) 扫描已完成中转(relay_uploaded)的 mediaAsset，创建 dispatchTask（并在真实组场景创建/更新 dispatchGroupTask）。
 *
 * 核心概念：
 * - 单条派发（dispatch-send）：每条 dispatchTask 对应一个 mediaAsset，走 handleDispatchJob（AI 文案生成/分类/发送）。
 * - 组派发（dispatch-send-group）：一批 dispatchTask 共享同一个真实 groupKey，走 handleDispatchGroupJob（sendMediaGroup）。
 * - 合集顺序闸门：合集条目必须按 episodeNo 顺序派发，未满足时延后（或在满足自动旁路条件时标记跳过）。
 *
 * 重要约束（本文件修复点）：
 * - 单资产任务不得写入伪 groupKey（如 asset-<id>、single-<id>）到 dispatchTask.groupKey，否则会被误判为组派发。
 * - 只有“真实组”（grouped-* 或组内任务数 >= TYPEB_GROUP_SEND_MIN_MEDIA_COUNT）才允许走组派发。
 */

const DISPATCH_HEAD_BYPASS_RETRY_THRESHOLD = 2;
const DISPATCH_HEAD_BYPASS_DELAY_SEC = 10 * 60;
const COLLECTION_SKIP_GRACE_MS = 5 * 60 * 1000;
const COLLECTION_SKIP_REASON = 'auto_skip_missing_after_grace';

type CollectionGateEvalStats = {
  autoBypassCount: number;
  blockedCount: number;
};

type DispatchGroupTaskSnapshot = {
  id: bigint;
  sourceExpectedCount: number | null;
};

type DispatchGroupTaskUpsertResult = {
  id: bigint;
  scheduleSlot: Date;
  readyDeadlineAt: Date | null;
  expectedMediaCount: number;
  sourceExpectedCount: number | null;
  actualUploadedCount: number;
  lastArrivalAt: Date | null;
  sealedAt: Date | null;
  sealReason: string | null;
};

/**
 * 从文件名/文本中尽量解析 episodeNo（用于合集集号解析失败的兜底修复）。
 * 注意：这里只是“尝试性”解析，不保证覆盖所有命名风格。
 */
function parseEpisodeNoFromText(text: string) {
  const patterns = [
    /\[第\s*(\d+)\s*(?:集|话|話)\]/,
    /第\s*(\d+)\s*(?:集|话|話)/,
    /S\d+E(\d+)/i,
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match || !match[1]) continue;
    const parsed = Number(match[1]);
    if (!Number.isFinite(parsed) || parsed <= 0) continue;
    return parsed;
  }
  return null;
}

/**
 * 频道发送频率控制：根据 lastPostAt + postIntervalSec 计算下次允许派发时间。
 * 用于避免同频道短时间内高频发消息触发风控/刷屏。
 */
function computeDispatchNextAllowedAt(args: {
  lastPostAt: Date | null;
  postIntervalSec: number;
  now: Date;
}) {
  if (!args.lastPostAt) return args.now;
  return new Date(args.lastPostAt.getTime() + Math.max(0, args.postIntervalSec) * 1000);
}

/**
 * 解析 mediaAsset.sourceMeta 中的合集元信息。
 * 规则：sourceMeta.isCollection === true 且 collectionName 非空时才视为合集。
 */
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
  const episodeParseFailed = meta.episodeParseFailed === true;

  if (!collectionName) return null;
  return {
    collectionName,
    episodeNo,
    episodeParseFailed,
  };
}

/**
 * 从 pathNormalized / localPath 中提取“真实组”的 groupKey。
 *
 * 重要：这里只识别 grouped-*，不再识别 single-*：
 * - single-* 是单条素材目录标识，不是“组发送”的 groupKey
 * - 误识别会导致单视频被调度到 group handler，进而派发失败
 */
function extractSecondaryGroupKeyFromPath(pathLike: string | null | undefined) {
  if (!pathLike) return null;
  const normalized = pathLike.replace(/\\/g, '/');
  const matched = normalized.match(/\/(grouped-\d+)\//i);
  return matched?.[1] ?? null;
}

/** 判断路径是否位于 grouped-* 目录下（用于防止 grouped 目录缺 groupKey 时错误降级为单发）。 */
function isGroupedPath(pathLike: string | null | undefined) {
  if (!pathLike) return false;
  return /\/(grouped-\d+)\//i.test(pathLike.replace(/\\/g, '/'));
}

/** 只把 grouped-* 视为“真实组” groupKey 格式。 */
function isGroupedStyleGroupKey(groupKey: string | null | undefined) {
  if (!groupKey) return false;
  return /^grouped-\d+$/i.test(groupKey.trim());
}

/**
 * 从 sourceMeta 中读取源组预期总数（sourceExpectedCount）。
 * 这是组发送门禁需要的关键字段：没有它就不能放行组派发。
 */
function parseSourceExpectedCount(sourceMeta: unknown) {
  if (!sourceMeta || typeof sourceMeta !== 'object' || Array.isArray(sourceMeta)) return null;
  const meta = sourceMeta as Record<string, unknown>;
  const raw = meta.sourceExpectedCount;
  if (typeof raw === 'number' && Number.isFinite(raw) && raw > 0) return Math.floor(raw);
  if (typeof raw === 'string' && /^\d+$/.test(raw) && Number(raw) > 0) return Math.floor(Number(raw));
  return null;
}

/**
 * 从 channel.navReplyMarkup 中读取合集调度配置。
 * 这是一个“可选扩展配置”，没有则返回默认值。
 */
function parseCollectionSchedulerConfig(navReplyMarkup: unknown) {
  const fallback = {
    collectionDispatchGateEnabled: true,
    collectionHeadBypassEnabled: false,
    collectionHeadBypassMinutes: 180,
  };

  if (!navReplyMarkup || typeof navReplyMarkup !== 'object' || Array.isArray(navReplyMarkup)) {
    return fallback;
  }

  const root = navReplyMarkup as Record<string, unknown>;
  const cfgRaw = root.__collectionConfig;
  if (!cfgRaw || typeof cfgRaw !== 'object' || Array.isArray(cfgRaw)) {
    return fallback;
  }

  const cfg = cfgRaw as Record<string, unknown>;
  const collectionDispatchGateEnabled =
    typeof cfg.collectionDispatchGateEnabled === 'boolean'
      ? cfg.collectionDispatchGateEnabled
      : fallback.collectionDispatchGateEnabled;
  const collectionHeadBypassEnabled =
    typeof cfg.collectionHeadBypassEnabled === 'boolean'
      ? cfg.collectionHeadBypassEnabled
      : fallback.collectionHeadBypassEnabled;
  const collectionHeadBypassMinutes =
    typeof cfg.collectionHeadBypassMinutes === 'number' && cfg.collectionHeadBypassMinutes > 0
      ? Math.floor(cfg.collectionHeadBypassMinutes)
      : fallback.collectionHeadBypassMinutes;

  return {
    collectionDispatchGateEnabled,
    collectionHeadBypassEnabled,
    collectionHeadBypassMinutes,
  };
}

/**
 * 合集顺序闸门：
 * - 如果当前要派发的 episodeNo 前面存在未成功派发的 episode，则阻塞当前 episode 派发
 * - 支持自动旁路（max retries / 超时）与缺失跳过（failed/deleted 且超过 grace）
 *
 * 返回：
 * - allowed=true：允许派发
 * - allowed=false：阻塞派发，blockedByEpisodeNo 指示阻塞来源集号
 */
async function canDispatchCollectionEpisode(args: {
  channelId: bigint;
  collectionName: string;
  episodeNo: number;
  now: Date;
  stats?: CollectionGateEvalStats;
}) {
  const collectionAssets = await withPrismaRetry(
    () =>
      prisma.mediaAsset.findMany({
        where: {
          channelId: args.channelId,
          sourceMeta: {
            path: ['collectionName'],
            equals: args.collectionName,
          },
        },
        select: {
          id: true,
          sourceMeta: true,
          status: true,
          updatedAt: true,
        },
      }),
    { label: 'dispatch.canDispatchCollectionEpisode.collectionAssets' },
  );

  // 只取“当前 episodeNo 之前”的集，作为顺序闸门的候选阻塞者。
  const prevEpisodes = collectionAssets
    .map((asset) => ({ id: asset.id, meta: parseCollectionMeta(asset.sourceMeta), status: asset.status, updatedAt: asset.updatedAt }))
    .filter((item) => item.meta && !item.meta.episodeParseFailed && item.meta.episodeNo !== null)
    .filter((item) => (item.meta?.episodeNo ?? 0) < args.episodeNo) as Array<{
    id: bigint;
    meta: { collectionName: string; episodeNo: number | null; episodeParseFailed: boolean };
    status: MediaStatus;
    updatedAt: Date;
  }>;

  if (prevEpisodes.length === 0) {
    return { allowed: true as const, blockedByEpisodeNo: null as number | null };
  }

  const prevEpisodeAssetIds = prevEpisodes.map((item) => item.id);

  // 前置集是否已有 dispatch success（成功即视为通过，不阻塞）。
  const successRows = await withPrismaRetry(
    () =>
      prisma.dispatchTask.findMany({
        where: {
          mediaAssetId: { in: prevEpisodeAssetIds },
          status: TaskStatus.success,
        },
        select: { mediaAssetId: true },
        distinct: ['mediaAssetId'],
      }),
    { label: 'dispatch.canDispatchCollectionEpisode.successRows' },
  );

  const successSet = new Set(successRows.map((row) => row.mediaAssetId.toString()));

  // 获取前置集最近一次 dispatch 结果，用于自动旁路（max retries / 超时）的判断。
  const latestDispatchRows = await withPrismaRetry(
    () =>
      prisma.dispatchTask.findMany({
        where: {
          mediaAssetId: { in: prevEpisodeAssetIds },
        },
        orderBy: [{ mediaAssetId: 'asc' }, { id: 'desc' }],
        select: {
          id: true,
          mediaAssetId: true,
          status: true,
          retryCount: true,
          maxRetries: true,
          updatedAt: true,
        },
      }),
    { label: 'dispatch.canDispatchCollectionEpisode.latestDispatchRows' },
  );

  const latestDispatchMap = new Map<string, {
    id: bigint;
    status: TaskStatus;
    retryCount: number;
    maxRetries: number;
    updatedAt: Date;
  }>();

  for (const row of latestDispatchRows) {
    const key = row.mediaAssetId.toString();
    if (latestDispatchMap.has(key)) continue;
    latestDispatchMap.set(key, {
      id: row.id,
      status: row.status,
      retryCount: row.retryCount,
      maxRetries: row.maxRetries,
      updatedAt: row.updatedAt,
    });
  }

  for (const prev of prevEpisodes) {
    if (successSet.has(prev.id.toString())) continue;

    const assetIdStr = prev.id.toString();
    const latestDispatch = latestDispatchMap.get(assetIdStr);
    const latestDispatchElapsedMs = latestDispatch
      ? args.now.getTime() - latestDispatch.updatedAt.getTime()
      : null;

    if (COLLECTION_AUTO_BYPASS_ENABLED) {
      // 满足条件则把“前置集”标记为 skipStatus，以允许后续集继续派发。
      const hitMaxRetries =
        COLLECTION_AUTO_BYPASS_ON_MAX_RETRIES &&
        latestDispatch &&
        latestDispatch.status !== TaskStatus.success &&
        latestDispatch.retryCount >= latestDispatch.maxRetries;

      const hitTimeout =
        latestDispatch &&
        latestDispatch.status !== TaskStatus.success &&
        Number.isFinite(latestDispatchElapsedMs) &&
        (latestDispatchElapsedMs as number) >= COLLECTION_AUTO_BYPASS_AFTER_MINUTES * 60 * 1000;

      if (hitMaxRetries || hitTimeout) {
        const sourceMeta = collectionAssets.find((asset) => asset.id === prev.id)?.sourceMeta;
        const metaObj = sourceMeta && typeof sourceMeta === 'object'
          ? (sourceMeta as Record<string, unknown>)
          : {};

        await withPrismaRetry(
          () =>
            prisma.mediaAsset.update({
              where: { id: prev.id },
              data: {
                sourceMeta: {
                  ...metaObj,
                  skipStatus: COLLECTION_AUTO_BYPASS_MARK,
                  skipReason: 'auto_bypass_after_retry_or_timeout',
                  skipAt: args.now.toISOString(),
                },
              },
            }),
          { label: 'dispatch.collection.autoBypass.updateMediaAsset' },
        );

        await withPrismaRetry(
          () =>
            prisma.riskEvent.create({
              data: {
                level: 'medium',
                eventType: 'collection_episode_auto_bypass',
                channelId: args.channelId,
                dispatchTaskId: latestDispatch?.id,
                payload: {
                  collectionName: args.collectionName,
                  blockedByEpisodeNo: prev.meta.episodeNo,
                  autoBypassAfterMinutes: COLLECTION_AUTO_BYPASS_AFTER_MINUTES,
                  reason: hitMaxRetries ? 'max_retries_exhausted' : 'timeout_exceeded',
                },
              },
            }),
          { label: 'dispatch.collection.autoBypass.riskEvent' },
        );

        if (args.stats) args.stats.autoBypassCount += 1;

        logger.warn('[scheduler] 合集顺序闸门自动旁路前置集', {
          channelId: args.channelId.toString(),
          collectionName: args.collectionName,
          blockedByEpisodeNo: prev.meta.episodeNo,
          mediaAssetId: assetIdStr,
          reason: hitMaxRetries ? 'max_retries_exhausted' : 'timeout_exceeded',
          collection_gate_auto_bypass_total: 1,
          collectionAutoBypassEnabled: COLLECTION_AUTO_BYPASS_ENABLED,
          collectionAutoBypassAfterMinutes: COLLECTION_AUTO_BYPASS_AFTER_MINUTES,
          collectionAutoBypassOnMaxRetries: COLLECTION_AUTO_BYPASS_ON_MAX_RETRIES,
        });

        continue;
      }
    }

    // 前置集如果已经 failed/deleted，且超过 grace，则标记 skipped_missing，避免无限阻塞后续集。
    const isFailedOrDeleted = prev.status === MediaStatus.failed || prev.status === MediaStatus.deleted;
    if (!isFailedOrDeleted) continue;

    const elapsedMs = args.now.getTime() - prev.updatedAt.getTime();
    if (Number.isFinite(elapsedMs) && elapsedMs >= COLLECTION_SKIP_GRACE_MS) {
      const sourceMeta = collectionAssets.find((asset) => asset.id === prev.id)?.sourceMeta;
      const metaObj = sourceMeta && typeof sourceMeta === 'object'
        ? (sourceMeta as Record<string, unknown>)
        : {};

      await withPrismaRetry(
        () =>
          prisma.mediaAsset.update({
            where: { id: prev.id },
            data: {
              sourceMeta: {
                ...metaObj,
                skipStatus: 'skipped_missing',
                skipReason: COLLECTION_SKIP_REASON,
                skipAt: args.now.toISOString(),
              },
            },
          }),
        { label: 'dispatch.collection.skipMissing.updateMediaAsset' },
      );
    }
  }

  const refreshedAssets = await withPrismaRetry(
    () =>
      prisma.mediaAsset.findMany({
        where: { id: { in: prevEpisodeAssetIds } },
        select: { id: true, sourceMeta: true },
      }),
    { label: 'dispatch.canDispatchCollectionEpisode.refreshedAssets' },
  );

  // 重新读取后，从 sourceMeta 里识别已跳过的集，构造 skippedSet。
  const skippedSet = new Set(
    refreshedAssets
      .filter((asset) => {
        const meta = asset.sourceMeta && typeof asset.sourceMeta === 'object'
          ? (asset.sourceMeta as Record<string, unknown>)
          : {};
        return meta.skipStatus === 'skipped_missing' || meta.skipStatus === COLLECTION_AUTO_BYPASS_MARK;
      })
      .map((asset) => asset.id.toString()),
  );

  // 找到最小的“未成功且未跳过”的前置集作为阻塞源。
  const blocked = prevEpisodes
    .filter((item) => !successSet.has(item.id.toString()) && !skippedSet.has(item.id.toString()))
    .sort((a, b) => (a.meta.episodeNo ?? 0) - (b.meta.episodeNo ?? 0))[0];

  if (!blocked) {
    return { allowed: true as const, blockedByEpisodeNo: null as number | null };
  }

  if (args.stats) args.stats.blockedCount += 1;

  return {
    allowed: false as const,
    blockedByEpisodeNo: blocked.meta.episodeNo ?? null,
  };
}

/**
 * 扫描 due dispatchTask 并入队 q_dispatch。
 *
 * 主要逻辑：
 * 1) 合集顺序闸门（可阻塞/旁路/跳过）
 * 2) 频道发送间隔限制
 * 3) 频道锁（同一频道同一时间只入队一个任务）
 * 4) 分发路径判断：
 *    - 默认单条（dispatch-send）
 *    - 只有真实组才走组派发（dispatch-send-group）
 */
export async function scheduleDueDispatchTasks() {
  const now = new Date();
  const gateStats: CollectionGateEvalStats = {
    autoBypassCount: 0,
    blockedCount: 0,
  };

  const dueTasks = await withPrismaRetry(
    () =>
      prisma.dispatchTask.findMany({
        where: {
          status: { in: [TaskStatus.pending, TaskStatus.scheduled, TaskStatus.failed] },
          nextRunAt: { lte: now },
        },
        orderBy: [{ priority: 'asc' }, { nextRunAt: 'asc' }],
        take: MAX_SCHEDULE_BATCH,
        select: {
          id: true,
          status: true,
          channelId: true,
          mediaAssetId: true,
          groupKey: true,
          retryCount: true,
          maxRetries: true,
          nextRunAt: true,
          scheduleSlot: true,
          channel: {
            select: {
              postIntervalSec: true,
              lastPostAt: true,
              navReplyMarkup: true,
            },
          },
          mediaAsset: {
            select: {
              sourceMeta: true,
              originalName: true,
              localPath: true,
              pathNormalized: true,
            },
          },
        },
      }),
    { label: 'dispatch.scheduleDueDispatchTasks.findDueTasks' },
  );

  const queuedChannelIds = new Set<string>();
  let queuedCount = 0;
  let groupedQueuedCount = 0;
  let groupedFallbackCount = 0;

  for (const task of dueTasks) {
    const channelIdStr = task.channelId.toString();

    if (queuedChannelIds.has(channelIdStr)) {
      continue;
    }

    // 【乐观锁防重】避免多个并发调度 tick 同时捞到同一条任务，导致门禁判定被狂暴重复执行
    // 原理：先把 nextRunAt 向后推 30 秒"占住"这条任务，防止其他 tick 同时拿到它
    // 注意：这 30 秒只是碰撞保护兜底，后续各个门禁分支（合集闸门/频道窗口等）
    //       都会用自己的正确延迟值覆写 nextRunAt（如 60s），不会真的等 30 秒
    //       只有进程在判定中途崩溃时，才会触发这 30 秒的兜底恢复
    const claimRes = await withPrismaRetry(
      () =>
        prisma.dispatchTask.updateMany({
          where: {
            id: task.id,
            nextRunAt: { lte: now },
          },
          data: {
            nextRunAt: new Date(Date.now() + 30 * 1000),
          },
        }),
      { label: 'dispatch.scheduleDueDispatchTasks.claimLock' },
    );

    if (claimRes.count === 0) {
      continue;
    }

    // 合集元信息用于顺序闸门；非合集则为 null。
    let collectionMeta = parseCollectionMeta(task.mediaAsset.sourceMeta);
    const collectionCfg = parseCollectionSchedulerConfig(task.channel.navReplyMarkup);

    // 合集集号解析失败时，尝试从文件名修复 episodeNo，避免长期阻塞。
    if (collectionMeta?.episodeParseFailed || (collectionMeta && collectionMeta.episodeNo === null)) {
      const fallbackEpisodeNo = parseEpisodeNoFromText(task.mediaAsset.originalName || '');
      if (fallbackEpisodeNo !== null) {
        const sourceMetaRaw = task.mediaAsset.sourceMeta;
        const sourceMetaObj =
          sourceMetaRaw && typeof sourceMetaRaw === 'object'
            ? (sourceMetaRaw as Record<string, unknown>)
            : {};

        await withPrismaRetry(
          () =>
            prisma.mediaAsset.update({
              where: { id: task.mediaAssetId },
              data: {
                sourceMeta: {
                  ...sourceMetaObj,
                  episodeNo: fallbackEpisodeNo,
                  episodeParseFailed: false,
                },
              },
            }),
          { label: 'dispatch.scheduleDueDispatchTasks.fixEpisodeNo' },
        );

        collectionMeta = {
          collectionName: collectionMeta.collectionName,
          episodeNo: fallbackEpisodeNo,
          episodeParseFailed: false,
        };

        logger.info('[scheduler] 合集集号已从文件名自动修复', {
          taskId: task.id.toString(),
          channelId: channelIdStr,
          mediaAssetId: task.mediaAssetId.toString(),
          collectionName: collectionMeta.collectionName,
          episodeNo: fallbackEpisodeNo,
          originalName: task.mediaAsset.originalName,
        });
      }
    }

    const shouldBypassHead =
      !collectionMeta &&
      task.status === TaskStatus.failed &&
      task.retryCount >= DISPATCH_HEAD_BYPASS_RETRY_THRESHOLD &&
      task.retryCount < task.maxRetries;

    // 头阻塞旁路：非合集任务若反复失败，可延后以让后面的任务有机会先发（减少“头部卡死”）。
    if (shouldBypassHead) {
      const bypassNextRunAt = new Date(Date.now() + DISPATCH_HEAD_BYPASS_DELAY_SEC * 1000);
      await withPrismaRetry(
        () =>
          prisma.dispatchTask.update({
            where: { id: task.id },
            data: {
              status: TaskStatus.failed,
              nextRunAt: bypassNextRunAt,
            },
          }),
        { label: 'dispatch.scheduleDueDispatchTasks.bypassHead' },
      );

      logger.warn('[scheduler] 分发头阻塞旁路，临时延后高重试任务', {
        taskId: task.id.toString(),
        channelId: channelIdStr,
        retryCount: task.retryCount,
        maxRetries: task.maxRetries,
        bypassNextRunAt: bypassNextRunAt.toISOString(),
      });

      continue;
    }

    if (collectionMeta?.episodeParseFailed) {
      await withPrismaRetry(
        () =>
          prisma.dispatchTask.update({
            where: { id: task.id },
            data: {
              status: TaskStatus.scheduled,
              nextRunAt: new Date(Date.now() + 10 * 60 * 1000),
            },
          }),
        { label: 'dispatch.scheduleDueDispatchTasks.episodeParseFailedDelay' },
      );

      logger.warn('[scheduler] 合集集号解析失败，阻塞分发等待人工改名重扫', {
        taskId: task.id.toString(),
        channelId: channelIdStr,
        mediaAssetId: task.mediaAssetId.toString(),
        collectionName: collectionMeta.collectionName,
      });
      continue;
    }

    // 合集顺序闸门：episodeNo 必须按顺序派发。
    if (
      collectionCfg.collectionDispatchGateEnabled &&
      collectionMeta &&
      !collectionMeta.episodeParseFailed &&
      collectionMeta.episodeNo !== null
    ) {
      const gateResult = await canDispatchCollectionEpisode({
        channelId: task.channelId,
        collectionName: collectionMeta.collectionName,
        episodeNo: collectionMeta.episodeNo,
        now,
        stats: gateStats,
      });

      if (!gateResult.allowed) {
        const bypassReady =
          collectionCfg.collectionHeadBypassEnabled &&
          task.status === TaskStatus.failed &&
          task.retryCount >= DISPATCH_HEAD_BYPASS_RETRY_THRESHOLD;

        // 合集头部旁路：如果前置集长期失败，允许延后本集的重试窗口（而不是频繁空转）。
        if (bypassReady) {
          const bypassNextRunAt = new Date(
            Date.now() + collectionCfg.collectionHeadBypassMinutes * 60 * 1000,
          );
          await withPrismaRetry(
            () =>
              prisma.dispatchTask.update({
                where: { id: task.id },
                data: {
                  status: TaskStatus.failed,
                  nextRunAt: bypassNextRunAt,
                },
              }),
            { label: 'dispatch.scheduleDueDispatchTasks.collectionBypassHead' },
          );

          logger.warn('[scheduler] 合集头部阻塞旁路生效，延后重试', {
            taskId: task.id.toString(),
            channelId: channelIdStr,
            mediaAssetId: task.mediaAssetId.toString(),
            collectionName: collectionMeta.collectionName,
            episodeNo: collectionMeta.episodeNo,
            blockedByEpisodeNo: gateResult.blockedByEpisodeNo,
            collectionHeadBypassMinutes: collectionCfg.collectionHeadBypassMinutes,
            bypassNextRunAt: bypassNextRunAt.toISOString(),
          });
          continue;
        }

        await withPrismaRetry(
          () =>
            prisma.dispatchTask.update({
              where: { id: task.id },
              data: {
                status: TaskStatus.scheduled,
                nextRunAt: new Date(Date.now() + 60 * 1000),
              },
            }),
          { label: 'dispatch.scheduleDueDispatchTasks.collectionGateDelay' },
        );

        logger.info('[scheduler] 合集顺序闸门阻塞当前分发任务', {
          taskId: task.id.toString(),
          channelId: channelIdStr,
          mediaAssetId: task.mediaAssetId.toString(),
          collectionName: collectionMeta.collectionName,
          episodeNo: collectionMeta.episodeNo,
          blockedByEpisodeNo: gateResult.blockedByEpisodeNo,
        });
        continue;
      }
    }

    // 频道发送间隔限制：未到窗口则延后。
    if (DISPATCH_CHANNEL_INTERVAL_GUARD_ENABLED) {
      const nextAllowedAt = computeDispatchNextAllowedAt({
        lastPostAt: task.channel.lastPostAt,
        postIntervalSec: task.channel.postIntervalSec,
        now,
      });

      if (nextAllowedAt.getTime() > now.getTime()) {
        await withPrismaRetry(
          () =>
            prisma.dispatchTask.update({
              where: { id: task.id },
              data: {
                status: TaskStatus.scheduled,
                nextRunAt: nextAllowedAt,
              },
            }),
          { label: 'dispatch.scheduleDueDispatchTasks.channelWindowDelay' },
        );

        logger.info('[scheduler] 分发任务未到频道发送窗口，已延后', {
          taskId: task.id.toString(),
          channelId: channelIdStr,
          postIntervalSec: task.channel.postIntervalSec,
          lastPostAt: task.channel.lastPostAt?.toISOString() ?? null,
          nextAllowedAt: nextAllowedAt.toISOString(),
        });
        continue;
      }
    }

    const lock = await tryAcquireChannelLock({
      scope: 'dispatch',
      channelId: task.channelId,
    });

    // 同频道同一时间只入队一个，避免并发派发造成频道频控/顺序混乱。
    if (!lock.acquired) {
      // 未抢到频道并发锁，将之前上的 5 分钟排他锁恢复为较短的回退时间 (10秒)，让其稍后再试
      await withPrismaRetry(
        () =>
          prisma.dispatchTask.update({
            where: { id: task.id },
            data: { nextRunAt: new Date(Date.now() + 10 * 1000) },
          }),
        { label: 'dispatch.scheduleDueDispatchTasks.channelLockRetry' },
      );

      logger.info('[scheduler] 分发任务跳过（频道锁未获取）', {
        taskId: task.id.toString(),
        channelId: channelIdStr,
        lockKey: lock.lockKey,
      });
      continue;
    }

    try {
      // CAS 标记任务为 scheduled，避免并发重复入队。
      const updated = await withPrismaRetry(
        () =>
          prisma.dispatchTask.updateMany({
            where: {
              id: task.id,
              status: {
                in: [TaskStatus.pending, TaskStatus.scheduled, TaskStatus.failed],
              },
            },
            data: {
              status: TaskStatus.scheduled,
            },
          }),
        { label: 'dispatch.scheduleDueDispatchTasks.markScheduled' },
      );

      if (updated.count === 0) continue;

      let groupSize = 1;
      let queueJobName: 'dispatch-send' | 'dispatch-send-group' = 'dispatch-send';
      let queueJobId = `dispatch-${task.id.toString()}`;
      let groupEnqueueGranted = true;

      if (task.groupKey) {
        // 注意：task.groupKey 只表示“可能属于组”，必须进一步判断是否为真实组。
        const groupedTasks = await withPrismaRetry(
          () =>
            prisma.dispatchTask.findMany({
              where: {
                channelId: task.channelId,
                groupKey: task.groupKey,
                status: { in: [TaskStatus.pending, TaskStatus.scheduled, TaskStatus.failed, TaskStatus.running] },
              },
              select: {
                id: true,
                mediaAssetId: true,
                scheduleSlot: true,
                mediaAsset: {
                  select: {
                    sourceMeta: true,
                  },
                },
              },
            }),
          { label: 'dispatch.scheduleDueDispatchTasks.findGroupedTasks' },
        );

        groupSize = groupedTasks.length;
        const shouldUseGroupDispatch =
          isGroupedStyleGroupKey(task.groupKey) || groupedTasks.length >= TYPEB_GROUP_SEND_MIN_MEDIA_COUNT;

        // 非真实组（例如历史遗留的伪 groupKey）统一强制单条派发，避免误入 group handler。
        if (!shouldUseGroupDispatch) {
          logger.info('[typeb_group] 非真实组任务，强制走单条派发', {
            taskId: task.id.toString(),
            channelId: task.channelId.toString(),
            mediaAssetId: task.mediaAssetId.toString(),
            groupKey: task.groupKey,
            groupSize: groupedTasks.length,
            dispatchPath: 'single',
            reason: 'non_real_group_key_or_insufficient_group_size',
          });
        } else {

          // 组派发门禁：统计组内“应该存在的媒体数”和“已上传完成的媒体数”，并结合 sourceExpectedCount 放行。
          const expectedMediaCount = await withPrismaRetry(
            () =>
              prisma.mediaAsset.count({
                where: {
                  channelId: task.channelId,
                  OR: [
                    {
                      sourceMeta: {
                        path: ['groupKey'],
                        equals: String(task.groupKey),
                      },
                    },
                    {
                      pathNormalized: {
                        contains: `/${task.groupKey}/`,
                      },
                    },
                    {
                      localPath: {
                        contains: `\\${task.groupKey}\\`,
                      },
                    },
                  ],
                  status: {
                    in: [
                      MediaStatus.ready,
                      MediaStatus.ingesting,
                      MediaStatus.relay_uploaded,
                    ],
                  },
                },
              }),
            { label: 'dispatch.scheduleDueDispatchTasks.countGroupExpectedAssets' },
          );

          const readyCount =
            groupedTasks.length > 0
              ? await withPrismaRetry(
                  () =>
                    prisma.mediaAsset.count({
                      where: {
                        id: { in: groupedTasks.map((t) => t.mediaAssetId) },
                        status: MediaStatus.relay_uploaded,
                        telegramFileId: { not: null },
                        dispatchMediaType: { not: null },
                      },
                    }),
                  { label: 'dispatch.scheduleDueDispatchTasks.countGroupReadyAssets' },
                )
              : 0;

          const uploadedCount = await withPrismaRetry(
            () =>
              prisma.mediaAsset.count({
                where: {
                  channelId: task.channelId,
                  OR: [
                    {
                      sourceMeta: {
                        path: ['groupKey'],
                        equals: String(task.groupKey),
                      },
                    },
                    {
                      pathNormalized: {
                        contains: `/${task.groupKey}/`,
                      },
                    },
                    {
                      localPath: {
                        contains: `\\${task.groupKey}\\`,
                      },
                    },
                  ],
                  status: MediaStatus.relay_uploaded,
                  telegramFileId: { not: null },
                  dispatchMediaType: { not: null },
                },
              }),
            { label: 'dispatch.scheduleDueDispatchTasks.countGroupUploadedAssets' },
          );

          const channelInWhitelist =
            TYPEB_GROUP_SEND_WHITELIST_CHANNEL_IDS.length === 0 ||
            TYPEB_GROUP_SEND_WHITELIST_CHANNEL_IDS.includes(task.channelId.toString());

          // 组发送开关/白名单未开启时，不允许降级单发（避免 grouped 语义被破坏），只延后重试。
          if (!TYPEB_GROUP_SEND_ENABLED || !channelInWhitelist) {
            await withPrismaRetry(
              () =>
                prisma.dispatchTask.update({
                  where: { id: task.id },
                  data: {
                    status: TaskStatus.scheduled,
                    nextRunAt: new Date(Date.now() + TYPEB_GROUP_RETRY_CHECK_MS),
                  },
                }),
              { label: 'dispatch.scheduleDueDispatchTasks.groupDisabledDelay' },
            );

            logger.warn('[typeb_group] grouped 任务不允许降级单发，等待组发送配置生效', {
              taskId: task.id.toString(),
              channelId: task.channelId.toString(),
              groupKey: task.groupKey,
              typebGroupSendEnabled: TYPEB_GROUP_SEND_ENABLED,
              channelInWhitelist,
            });
            continue;
          }

          const groupScheduleSlot = groupedTasks
            .map((t) => t.scheduleSlot)
            .sort((a, b) => a.getTime() - b.getTime())[0] ?? task.scheduleSlot;
          const existingGroupTaskSnapshot = await withPrismaRetry<DispatchGroupTaskSnapshot | null>(
            () =>
              (prisma as any).dispatchGroupTask.findFirst({
                where: {
                  channelId: task.channelId,
                  groupKey: task.groupKey,
                },
                orderBy: [{ updatedAt: 'desc' }, { id: 'desc' }],
                select: {
                  id: true,
                  sourceExpectedCount: true,
                },
              }),
            { label: 'dispatch.scheduleDueDispatchTasks.findExistingGroupTaskSnapshot' },
          );
          // 修复点：sourceExpectedCount 可能在 dispatchGroupTask 初次创建时缺失，
          // 若只读 existingGroupTaskSnapshot 会导致后续永远卡在 0。
          // 这里从同组任务关联 mediaAsset.sourceMeta 再聚合一次，作为“回填来源”。
          const groupedSourceExpectedCountCandidates = groupedTasks
            .map((t) => parseSourceExpectedCount(t.mediaAsset?.sourceMeta))
            .filter((n): n is number => typeof n === 'number' && n > 0);
          const groupedSourceExpectedCount =
            groupedSourceExpectedCountCandidates.length > 0
              ? Math.max(...groupedSourceExpectedCountCandidates)
              : 0;
          // 保持“取最大值”的保守策略：不降低已有 sourceExpectedCount，
          // 仅在缺失/偏小时做补写，避免分组闸门因 0 永久阻塞。
          const inheritedSourceExpectedCount = Math.max(
            0,
            Number(existingGroupTaskSnapshot?.sourceExpectedCount ?? 0),
            groupedSourceExpectedCount,
          );

          const groupTask = await withPrismaRetry<DispatchGroupTaskUpsertResult>(
            () =>
              (prisma as any).dispatchGroupTask.upsert({
                where: {
                  channelId_scheduleSlot_groupKey: {
                    channelId: task.channelId,
                    scheduleSlot: groupScheduleSlot,
                    groupKey: task.groupKey,
                  },
                },
                create: {
                  channelId: task.channelId,
                  groupKey: task.groupKey,
                  scheduleSlot: groupScheduleSlot,
                  status: TaskStatus.pending,
                  retryCount: 0,
                  maxRetries: task.maxRetries,
                  nextRunAt: now,
                  readyDeadlineAt: new Date(groupScheduleSlot.getTime() + TYPEB_GROUP_READY_TIMEOUT_MS),
                  expectedMediaCount: Math.max(expectedMediaCount, groupSize),
                  ...(inheritedSourceExpectedCount > 0
                    ? {
                        sourceExpectedCount: inheritedSourceExpectedCount,
                      }
                    : {}),
                  actualReadyCount: readyCount,
                  actualUploadedCount: uploadedCount,
                  lastArrivalAt: now,
                },
                update: {
                  nextRunAt: now,
                  expectedMediaCount: Math.max(expectedMediaCount, groupSize),
                  ...(inheritedSourceExpectedCount > 0
                    ? {
                        sourceExpectedCount: {
                          set: inheritedSourceExpectedCount,
                        },
                      }
                    : {}),
                  actualReadyCount: readyCount,
                  actualUploadedCount: uploadedCount,
                  readyDeadlineAt: {
                    set: new Date(groupScheduleSlot.getTime() + TYPEB_GROUP_READY_TIMEOUT_MS),
                  },
                },
                select: {
                  id: true,
                  scheduleSlot: true,
                  readyDeadlineAt: true,
                  expectedMediaCount: true,
                  sourceExpectedCount: true,
                  actualUploadedCount: true,
                  lastArrivalAt: true,
                  sealedAt: true,
                  sealReason: true,
                },
              }),
            { label: 'dispatch.scheduleDueDispatchTasks.upsertGroupTask' },
          );

          const sourceExpectedCount = Number(groupTask.sourceExpectedCount ?? 0);
          const expectedTotal = sourceExpectedCount;
          const persistedUploadedCount = Number(groupTask.actualUploadedCount ?? 0);
          const effectiveUploadedCount = Math.max(persistedUploadedCount, uploadedCount);

          // 门禁 1：没有 sourceExpectedCount 直接阻塞（这是你之前遇到的核心拦截条件）。
          if (expectedTotal <= 0) {
            await withPrismaRetry(
              () =>
                prisma.dispatchTask.update({
                  where: { id: task.id },
                  data: {
                    status: TaskStatus.scheduled,
                    nextRunAt: new Date(Date.now() + TYPEB_GROUP_RETRY_CHECK_MS),
                  },
                }),
              { label: 'dispatch.scheduleDueDispatchTasks.groupMissingSourceTotalDelay' },
            );

            logger.error('【分组闸门】缺少源组媒体总数，阻塞派发', {
              taskId: task.id.toString(),
              channelId: task.channelId.toString(),
              groupKey: task.groupKey,
              dispatchGroupTaskId: groupTask.id.toString(),
              sourceExpectedCount: expectedTotal,
              expectedMediaCount: Number(groupTask.expectedMediaCount ?? 0),
              actualUploadedCount: effectiveUploadedCount,
            });
            continue;
          }

          // 门禁 2：上传数超过预期总数，属于数据不一致，阻塞并告警。
          if (effectiveUploadedCount > expectedTotal) {
            await withPrismaRetry(
              () =>
                prisma.dispatchTask.update({
                  where: { id: task.id },
                  data: {
                    status: TaskStatus.scheduled,
                    nextRunAt: new Date(Date.now() + TYPEB_GROUP_RETRY_CHECK_MS),
                  },
                }),
              { label: 'dispatch.scheduleDueDispatchTasks.groupUploadedOverflowDelay' },
            );

            logger.error('【分组闸门】已上传数超过源总数，阻塞并告警', {
              taskId: task.id.toString(),
              channelId: task.channelId.toString(),
              groupKey: task.groupKey,
              dispatchGroupTaskId: groupTask.id.toString(),
              sourceExpectedCount: expectedTotal,
              actualUploadedCount: effectiveUploadedCount,
            });
            continue;
          }

          // 门禁 3：上传未完成则等待。
          if (effectiveUploadedCount < expectedTotal) {
            await withPrismaRetry(
              () =>
                prisma.dispatchTask.update({
                  where: { id: task.id },
                  data: {
                    status: TaskStatus.scheduled,
                    nextRunAt: new Date(Date.now() + TYPEB_GROUP_RETRY_CHECK_MS),
                  },
                }),
              { label: 'dispatch.scheduleDueDispatchTasks.groupWaitingUploadDelay' },
            );

            logger.info('【分组闸门】组内上传未完成，继续等待', {
              taskId: task.id.toString(),
              channelId: task.channelId.toString(),
              groupKey: task.groupKey,
              dispatchGroupTaskId: groupTask.id.toString(),
              sourceExpectedCount: expectedTotal,
              actualUploadedCount: effectiveUploadedCount,
            });
            continue;
          }

          // 门禁放行后，使用 dispatchGroupTask 做一次 CAS 抢占，避免并发重复入队组发送。
          logger.info('【分组闸门】上传数与源总数一致，放行派发', {
            taskId: task.id.toString(),
            channelId: task.channelId.toString(),
            groupKey: task.groupKey,
            dispatchGroupTaskId: groupTask.id.toString(),
            sourceExpectedCount: expectedTotal,
            actualUploadedCount: effectiveUploadedCount,
          });

          const enqueueGate = await withPrismaRetry<{ count: number }>(
            () =>
              (prisma as any).dispatchGroupTask.updateMany({
                where: {
                  id: groupTask.id,
                  status: { in: [TaskStatus.pending, TaskStatus.scheduled, TaskStatus.failed] },
                },
                data: {
                  status: TaskStatus.scheduled,
                  nextRunAt: now,
                },
              }),
            { label: 'dispatch.scheduleDueDispatchTasks.groupEnqueueCas' },
          );

          groupEnqueueGranted = enqueueGate.count > 0;
          if (!groupEnqueueGranted) {
            logger.info('[typeb_group] grouped 入队被并发抢占，跳过本次重复入队', {
              channelId: task.channelId.toString(),
              groupKey: task.groupKey,
              dispatchGroupTaskId: groupTask.id.toString(),
              reason: 'group_enqueue_cas_rejected',
            });
            logger.info('[typeb_group][diag] enqueue cas rejected snapshot', {
              taskId: task.id.toString(),
              channelId: task.channelId.toString(),
              groupKey: task.groupKey,
              dispatchGroupTaskId: groupTask.id.toString(),
              expectedTotal,
              readyCount,
              uploadedCount: effectiveUploadedCount,
              now: now.toISOString(),
            });
            continue;
          }

          queueJobName = 'dispatch-send-group';
          queueJobId = `dispatch-group-${groupTask.id.toString()}`;

          logger.info('[typeb_group][diag] enqueue granted snapshot', {
            taskId: task.id.toString(),
            channelId: task.channelId.toString(),
            groupKey: task.groupKey,
            dispatchGroupTaskId: groupTask.id.toString(),
            expectedTotal,
            readyCount,
            uploadedCount: effectiveUploadedCount,
            queueJobName,
            queueJobId,
          });

          logger.info('[typeb_group][verify] expectedTotal source snapshot', {
            taskId: task.id.toString(),
            channelId: task.channelId.toString(),
            groupKey: task.groupKey,
            dispatchGroupTaskId: groupTask.id.toString(),
            sourceExpectedCount: Number(groupTask.sourceExpectedCount ?? 0),
            fallbackExpectedMediaCount: Number(groupTask.expectedMediaCount ?? 0),
            expectedTotal,
            readyCount,
            uploadedCount: effectiveUploadedCount,
          });
        }
      }

      // 真正入队到 q_dispatch（dispatch-send / dispatch-send-group）。
      await dispatchQueue.add(
        queueJobName,
        {
          dispatchTaskId: task.id.toString(),
          channelId: task.channelId.toString(),
          mediaAssetId: task.mediaAssetId.toString(),
          groupKey: task.groupKey,
          groupSize,
          retryCount: task.retryCount,
        },
        {
          jobId: queueJobId,
          removeOnComplete: true,
          removeOnFail: 200,
        },
      );

      queuedChannelIds.add(channelIdStr);
      queuedCount += 1;
      if (queueJobName === 'dispatch-send-group') groupedQueuedCount += 1;
      else if (task.groupKey) groupedFallbackCount += 1;

      if (task.groupKey) {
        logger.info('[typeb_group] grouped 入队决策', {
          channelId: task.channelId.toString(),
          groupKey: task.groupKey,
          groupSize,
          queueJobName,
          typeb_group_dispatch_path_total: 1,
          dispatchPath: queueJobName === 'dispatch-send-group' ? 'group' : 'single_fallback',
        });
      }
    } finally {
      await releaseChannelLock({ lockKey: lock.lockKey, lockToken: lock.lockToken });
    }
  }

  logger.info('[typeb_metrics] dispatch scheduler tick', {
    typeb_enqueue_total: queuedCount,
    typeb_group_send_queue_total: groupedQueuedCount,
    typeb_group_send_fallback_queue_total: groupedFallbackCount,
    collection_gate_auto_bypass_total: gateStats.autoBypassCount,
    collection_gate_block_total: gateStats.blockedCount,
    collectionAutoBypassEnabled: COLLECTION_AUTO_BYPASS_ENABLED,
    collectionAutoBypassAfterMinutes: COLLECTION_AUTO_BYPASS_AFTER_MINUTES,
    collectionAutoBypassOnMaxRetries: COLLECTION_AUTO_BYPASS_ON_MAX_RETRIES,
  });

  logger.info('catalog_source_write_metrics', {
    tag: 'catalog_source_write_metrics',
    message: 'catalog_source_item 写入指标快照',
    ...catalogSourceWriteMetrics,
    avgUpsertDurationMs:
      catalogSourceWriteMetrics.upsertSuccessTotal + catalogSourceWriteMetrics.upsertFailedTotal > 0
        ? Math.round(
            catalogSourceWriteMetrics.upsertDurationMsTotal /
              (catalogSourceWriteMetrics.upsertSuccessTotal + catalogSourceWriteMetrics.upsertFailedTotal),
          )
        : 0,
  });

  if (queuedCount > 0) {
    logger.info('[scheduler] 已入队分发任务', { count: queuedCount });
  }

  return {
    queuedCount,
    autoBypassCount: gateStats.autoBypassCount,
    blockedCount: gateStats.blockedCount,
  };
}

/**
 * 任务定义调度器入口：扫描 relay_uploaded 且未创建 dispatchTask 的 mediaAsset，创建 dispatchTask。
 *
 * 关键点：
 * - primaryGroupKey：来自 sourceMeta.groupKey（权威）
 * - secondaryGroupKey：从路径中推断（只允许 grouped-*）
 * - bucketGroupKey：内部用于“把属于同一组的一批资产聚在一起”的 key
 *   - 若缺 groupKey，会临时用 asset-<id> 做 bucket key
 *   - 但该值不会写入 dispatchTask.groupKey（避免单资产误被 group 化）
 */
export async function scheduleDispatchForDefinition(taskDefinitionId: bigint) {
  try {
    const definition = await getTaskDefinitionModel().findUnique({
      where: { id: taskDefinitionId },
      select: { priority: true },
    });

    if (!definition) {
      throw new Error(`未找到任务定义: ${taskDefinitionId}`);
    }

    const unscheduledAssets = await withPrismaRetry(
      () =>
        prisma.mediaAsset.findMany({
          where: {
            status: MediaStatus.relay_uploaded,
            telegramFileId: { not: null },
            dispatchTasks: {
              none: {},
            },
          },
          select: {
            id: true,
            channelId: true,
            originalName: true,
            sourceMeta: true,
            localPath: true,
            pathNormalized: true,
          },
          take: 200,
        }),
      { label: 'dispatch.scheduleDispatchForDefinition.findUnscheduledAssets' },
    );

    let createdCount = 0;
    let groupScheduledCount = 0;
    const now = new Date();

    const grouped = new Map<string, typeof unscheduledAssets>();
    for (const asset of unscheduledAssets) {
      const sourceMeta = (asset.sourceMeta ?? {}) as Record<string, unknown>;
      const primaryGroupKeyRaw = sourceMeta.groupKey;
      const primaryGroupKey =
        typeof primaryGroupKeyRaw === 'string' && primaryGroupKeyRaw.trim()
          ? primaryGroupKeyRaw.trim()
          : null;
      const secondaryGroupKey =
        extractSecondaryGroupKeyFromPath(asset.pathNormalized) ??
        extractSecondaryGroupKeyFromPath(asset.localPath);
      const groupedPathLike = asset.pathNormalized ?? asset.localPath;
      const fromGroupedPath = isGroupedPath(groupedPathLike);

      let groupKey = primaryGroupKey;
      if (!groupKey && secondaryGroupKey) {
        groupKey = secondaryGroupKey;
        // secondaryGroupKey 仅作为“grouped-*”路径场景的补丁来源；普通 single 目录不会进入这里。
        logger.info('[typeb_group] 使用 secondaryGroupKey 修复分组', {
          mediaAssetId: asset.id.toString(),
          channelId: asset.channelId.toString(),
          secondaryGroupKey,
          typeb_group_key_secondary_resolved_total: 1,
        });
      }

      if (primaryGroupKey && secondaryGroupKey && primaryGroupKey !== secondaryGroupKey) {
        logger.warn('[typeb_group] 检测到主次分组键不一致，按主键优先', {
          mediaAssetId: asset.id.toString(),
          channelId: asset.channelId.toString(),
          groupKeyPrimary: primaryGroupKey,
          groupKeySecondary: secondaryGroupKey,
          typeb_group_key_conflict_total: 1,
        });
      }

      if (!groupKey && fromGroupedPath) {
        // grouped 目录里必须能找到 groupKey，否则宁可跳过也不要误判为单发（避免 grouped 不完整时乱序）。
        logger.warn('[typeb_group] grouped 路径缺失可用 groupKey，阻止错误分组', {
          mediaAssetId: asset.id.toString(),
          channelId: asset.channelId.toString(),
          originalName: asset.originalName,
          pathLike: groupedPathLike,
          reason: 'group_key_missing_for_grouped_path',
          typeb_group_key_missing_total: 1,
        });
        continue;
      }

      // bucketGroupKey 是内部聚合 key：缺 groupKey 时使用 asset-<id>，仅用于把单资产放入独立 bucket。
      const bucketGroupKey = groupKey ?? `asset-${asset.id.toString()}`;
      const key = `${asset.channelId.toString()}::${bucketGroupKey}`;
      const bucket = grouped.get(key);
      if (bucket) bucket.push(asset);
      else grouped.set(key, [asset]);
    }

    for (const [key, assets] of grouped) {
      const [, bucketGroupKey] = key.split('::');
      const sorted = [...assets].sort((a, b) =>
        String(a.originalName || '').localeCompare(String(b.originalName || ''), 'zh-CN'),
      );
      // 这里再次用 sourceMeta.groupKey 做权威回读：bucketGroupKey 只在 grouped-* 场景作为 fallback。
      const sourceMeta = (sorted[0]?.sourceMeta ?? {}) as Record<string, unknown>;
      const primaryGroupKeyRaw = sourceMeta.groupKey;
      const primaryGroupKey =
        typeof primaryGroupKeyRaw === 'string' && primaryGroupKeyRaw.trim()
          ? primaryGroupKeyRaw.trim()
          : null;
      const groupKey = primaryGroupKey ?? (isGroupedStyleGroupKey(bucketGroupKey) ? bucketGroupKey : null);
      const groupSlot = new Date(now.getTime());
      const isRealGroup = sorted.length > 1;
      // 只有真实组才持久化 groupKey 到 dispatchTask，避免单资产误入组派发。
      const shouldPersistGroupKey = isRealGroup || isGroupedStyleGroupKey(groupKey);
      const sourceExpectedCountCandidates = sorted
        .map((asset) => parseSourceExpectedCount(asset.sourceMeta))
        .filter((n): n is number => typeof n === 'number' && n > 0);
      const groupSourceExpectedCount =
        sourceExpectedCountCandidates.length > 0
          ? Math.max(...sourceExpectedCountCandidates)
          : null;
      const existingGroupTaskSnapshot =
        isRealGroup && groupKey
          ? await withPrismaRetry<DispatchGroupTaskSnapshot | null>(
              () =>
                (prisma as any).dispatchGroupTask.findFirst({
                  where: {
                    channelId: sorted[0].channelId,
                    groupKey,
                  },
                  orderBy: [{ updatedAt: 'desc' }, { id: 'desc' }],
                  select: {
                    id: true,
                    sourceExpectedCount: true,
                  },
                }),
              { label: 'dispatch.scheduleDispatchForDefinition.findExistingGroupTaskSnapshot' },
            )
          : null;
      const inheritedSourceExpectedCount = Math.max(
        Number(groupSourceExpectedCount ?? 0),
        Number(existingGroupTaskSnapshot?.sourceExpectedCount ?? 0),
      );

      if (isGroupedStyleGroupKey(groupKey) && sorted.length === 1) {
        // grouped-* 单元素：一般意味着组还没抓齐/还在上传中，不降级为单发，等待后续补齐。
        logger.warn('[typeb_group] grouped 单元素组已识别，保持等待组装不降级单发', {
          channelId: sorted[0].channelId.toString(),
          groupKey,
          mediaAssetId: sorted[0].id.toString(),
          typeb_group_singleton_detected_total: 1,
        });
      }

      if (isRealGroup) {
        groupScheduledCount += 1;
      }

      if (isRealGroup && groupKey) {
        // 真实组才创建/更新 dispatchGroupTask（组发送门禁的数据载体）。
        await withPrismaRetry(
          () =>
            (prisma as any).dispatchGroupTask.upsert({
              where: {
                channelId_scheduleSlot_groupKey: {
                  channelId: sorted[0].channelId,
                  scheduleSlot: groupSlot,
                  groupKey,
                },
              },
              create: {
                channelId: sorted[0].channelId,
                groupKey,
                scheduleSlot: groupSlot,
                status: TaskStatus.pending,
                retryCount: 0,
                maxRetries: definition.priority ? 6 : 6,
                nextRunAt: groupSlot,
                readyDeadlineAt: new Date(groupSlot.getTime() + TYPEB_GROUP_READY_TIMEOUT_MS),
                expectedMediaCount: sorted.length,
                ...(inheritedSourceExpectedCount > 0
                  ? {
                      sourceExpectedCount: inheritedSourceExpectedCount,
                    }
                  : {}),
                actualReadyCount: 0,
                actualUploadedCount: 0,
                lastArrivalAt: now,
              },
              update: {
                nextRunAt: groupSlot,
                expectedMediaCount: sorted.length,
                ...(inheritedSourceExpectedCount > 0
                  ? {
                      sourceExpectedCount: {
                        set: inheritedSourceExpectedCount,
                      },
                    }
                  : {}),
                actualUploadedCount: {
                  set: 0,
                },
                lastArrivalAt: {
                  set: now,
                },
                readyDeadlineAt: new Date(groupSlot.getTime() + TYPEB_GROUP_READY_TIMEOUT_MS),
                updatedAt: new Date(),
              },
            }),
          { label: 'dispatch.scheduleDispatchForDefinition.upsertDispatchGroupTask' },
        );
      }

      for (let i = 0; i < sorted.length; i += 1) {
        const asset = sorted[i];
        const slot = new Date(groupSlot.getTime() + i * 1000);

        await withPrismaRetry(
          () =>
            prisma.dispatchTask.create({
              data: {
                channelId: asset.channelId,
                mediaAssetId: asset.id,
                // 关键修复点：单资产任务 groupKey 为 null；只有真实组才持久化 groupKey。
                groupKey: shouldPersistGroupKey ? groupKey : null,
                status: TaskStatus.pending,
                scheduleSlot: slot,
                plannedAt: slot,
                nextRunAt: slot,
                priority: definition.priority ?? 100,
              },
              select: { id: true },
            }),
          { label: 'dispatch.scheduleDispatchForDefinition.createDispatchTask' },
        );

        createdCount += 1;
      }
    }

    const tickSummary = await scheduleDueDispatchTasks();

    logger.info('[typeb_metrics] grouped scheduling summary', {
      typeb_group_scheduled_total: groupScheduledCount,
      typeb_group_asset_created_total: createdCount,
      metric_labels: {
        typeb_group_scheduled_total: 'TypeB 组级调度批次数',
        typeb_group_asset_created_total: 'TypeB 组级调度创建任务总数',
      },
    });

    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'success',
      summary: {
        executor: 'dispatch_send',
        createdTasks: createdCount,
        groupedBatches: groupScheduledCount,
        queuedCount: tickSummary.queuedCount,
        autoBypassCount: tickSummary.autoBypassCount,
        blockedCount: tickSummary.blockedCount,
        collectionAutoBypassEnabled: COLLECTION_AUTO_BYPASS_ENABLED,
        collectionAutoBypassAfterMinutes: COLLECTION_AUTO_BYPASS_AFTER_MINUTES,
        collectionAutoBypassOnMaxRetries: COLLECTION_AUTO_BYPASS_ON_MAX_RETRIES,
        message: '自动扫描并入队分发任务',
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    // eslint-disable-next-line no-console
    console.error(`[scheduler] 分发调度失败，taskDef=${taskDefinitionId}:`, error);
    await updateTaskDefinitionRunStatus({
      taskDefinitionId,
      status: 'failed',
      summary: { executor: 'dispatch_send', error: `分发调度失败: ${message}` },
    });
  }
}

/**
 * 开发环境故障注入测试脚本
 * 使用方法: ts-node src/test.ts -> main() -> 执行注入逻辑
 */

import { PrismaClient, MediaStatus } from '@prisma/client';
import { unlink } from 'node:fs/promises';
import { relayUploadQueue } from './infra/redis';

const prisma = new PrismaClient();

// 判断是否为类生产环境
function isProdLikeEnv() {
  return process.env.NODE_ENV === 'production';
}

// BigInt JSON 序列化替换器
function bigintJsonReplacer(_key: string, value: unknown) {
  return typeof value === 'bigint' ? value.toString() : value;
}

// 命令: 按名称查找资产
async function cmdFindByName(keyword: string) {
  const asset = await prisma.mediaAsset.findFirst({
    where: {
      originalName: {
        contains: keyword,
      },
    },
    include: {
      dispatchTasks: true,
      channel: true,
    },
    orderBy: { id: 'desc' },
  });

  console.log(JSON.stringify(asset, bigintJsonReplacer, 2));
}

// 命令: 注入“源文件缺失”故障
async function cmdInjectMissingFile(mediaAssetIdRaw: string) {
  const mediaAssetId = BigInt(mediaAssetIdRaw);
  const asset = await prisma.mediaAsset.findUnique({
    where: { id: mediaAssetId },
    select: { id: true, localPath: true, status: true },
  });

  if (!asset) throw new Error(`mediaAsset 不存在: ${mediaAssetIdRaw}`);

  try {
    await unlink(asset.localPath);
    console.log(`[inject:missing-file] 已删除源文件: ${asset.localPath}`);
  } catch (err: any) {
    if (err?.code === 'ENOENT') {
      console.log(`[inject:missing-file] 文件本就不存在，跳过: ${asset.localPath}`);
    } else {
      throw err;
    }
  }

  await prisma.mediaAsset.update({
    where: { id: mediaAssetId },
    data: {
      status: MediaStatus.ingesting,
      sourceMeta: {
        ingestStage: 'uploading',
        injectTag: 'fault_missing_file',
        injectAt: new Date().toISOString(),
      },
    },
  });

  console.log(`[inject:missing-file] 已标记为 ingesting，等待 worker/reconcile 自愈处理: ${mediaAssetIdRaw}`);
}

// 命令: 注入“ingesting 超时”故障
async function cmdInjectStaleIngesting(mediaAssetIdRaw: string, staleMinutesRaw?: string) {
  const mediaAssetId = BigInt(mediaAssetIdRaw);
  const staleMinutes = Number(staleMinutesRaw || '120');
  const staleAt = new Date(Date.now() - staleMinutes * 60 * 1000).toISOString();

  const asset = await prisma.mediaAsset.findUnique({
    where: { id: mediaAssetId },
    select: { id: true, sourceMeta: true },
  });
  if (!asset) throw new Error(`mediaAsset 不存在: ${mediaAssetIdRaw}`);

  const sourceMeta =
    asset.sourceMeta && typeof asset.sourceMeta === 'object'
      ? (asset.sourceMeta as Record<string, unknown>)
      : {};

  await prisma.mediaAsset.update({
    where: { id: mediaAssetId },
    data: {
      status: MediaStatus.ingesting,
      updatedAt: new Date(staleAt),
      sourceMeta: {
        ...sourceMeta,
        ingestStage: 'uploading',
        ingestLeaseUntil: staleAt,
        ingestLastHeartbeatAt: staleAt,
        injectTag: 'fault_stale_ingesting',
        injectAt: new Date().toISOString(),
      },
    },
  });

  console.log(`[inject:stale] 已注入 ingesting 超时场景: mediaAssetId=${mediaAssetIdRaw}, staleMinutes=${staleMinutes}`);
}

// 命令: 注入“队列堆积”故障
async function cmdInjectQueueBacklog(channelIdRaw: string, countRaw?: string) {
  const channelId = BigInt(channelIdRaw);
  const count = Math.max(1, Number(countRaw || '30'));

  const channel = await prisma.channel.findUnique({
    where: { id: channelId },
    select: { id: true, defaultBotId: true },
  });
  if (!channel) throw new Error(`channel 不存在: ${channelIdRaw}`);

  const relayChannel = await prisma.relayChannel.findFirst({
    where: {
      botId: channel.defaultBotId ?? undefined,
      isActive: true,
    },
    select: { id: true },
    orderBy: { id: 'asc' },
  });
  if (!relayChannel) throw new Error(`找不到可用 relayChannel: channelId=${channelIdRaw}`);

  const readyAssets = await prisma.mediaAsset.findMany({
    where: { channelId, status: MediaStatus.ready },
    select: { id: true },
    orderBy: { id: 'desc' },
    take: count,
  });

  if (readyAssets.length === 0) {
    throw new Error(`没有可用于堆积验证的 ready 资产: channelId=${channelIdRaw}`);
  }

  let enqueued = 0;
  for (const asset of readyAssets) {
    await relayUploadQueue.add(
      'relay_upload',
      {
        mediaAssetId: asset.id.toString(),
        relayChannelId: relayChannel.id.toString(),
      },
      {
        removeOnComplete: true,
        removeOnFail: 100,
      },
    );
    enqueued += 1;
  }

  console.log(`[inject:queue-backlog] 已注入队列堆积任务: channelId=${channelIdRaw}, count=${enqueued}`);
}

// 打印帮助信息
function printHelp() {
  console.log(`
用法（开发环境故障注入）:

1) 查询资产（保留旧功能）
   ts-node src/test.ts find-name <关键词>

2) 注入“源文件缺失”
   ts-node src/test.ts inject missing-file <mediaAssetId>

3) 注入“ingesting 超时”
   ts-node src/test.ts inject stale-ingesting <mediaAssetId> [staleMinutes=120]

4) 注入“队列堆积”
   ts-node src/test.ts inject queue-backlog <channelId> [count=30]
`);
}

// 主函数
async function main() {
  const [, , command, subCommand, ...rest] = process.argv;

  if (!command) {
    printHelp();
    return;
  }

  if (command === 'find-name') {
    const keyword = subCommand || '小崔说事';
    await cmdFindByName(keyword);
    return;
  }

  if (command === 'inject') {
    if (isProdLikeEnv()) {
      throw new Error('故障注入仅允许在非 production 环境执行');
    }

    if (subCommand === 'missing-file') {
      const mediaAssetId = rest[0];
      if (!mediaAssetId) throw new Error('缺少 mediaAssetId');
      await cmdInjectMissingFile(mediaAssetId);
      return;
    }

    if (subCommand === 'stale-ingesting') {
      const mediaAssetId = rest[0];
      const staleMinutes = rest[1];
      if (!mediaAssetId) throw new Error('缺少 mediaAssetId');
      await cmdInjectStaleIngesting(mediaAssetId, staleMinutes);
      return;
    }

    if (subCommand === 'queue-backlog') {
      const channelId = rest[0];
      const count = rest[1];
      if (!channelId) throw new Error('缺少 channelId');
      await cmdInjectQueueBacklog(channelId, count);
      return;
    }
  }

  printHelp();
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await prisma.$disconnect();
  });

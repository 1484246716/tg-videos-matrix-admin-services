/**
 * 资源选择工具：供 scheduler/worker/service 共享使用。
 * 提供随机选择 bot 与 relayChannel 的查询方法。
 */

import { prisma } from '../infra/prisma';

// 随机选择一个可用 bot。
export async function pickRandomBot() {
  const bots = await prisma.bot.findMany({
    where: { status: 'active' },
    select: { id: true, status: true, tokenEncrypted: true },
  });

  if (bots.length === 0) {
    throw new Error('没有可用机器人');
  }

  return bots[Math.floor(Math.random() * bots.length)];
}

// 随机选择一个可用 relay channel。
export async function pickRandomRelayChannel() {
  const relayChannels = await prisma.relayChannel.findMany({
    where: {
      isActive: true,
      bot: { status: 'active' },
    },
    include: {
      bot: { select: { id: true, status: true, tokenEncrypted: true } },
    },
  });

  if (relayChannels.length === 0) {
    throw new Error('没有可用中转频道');
  }

  return relayChannels[Math.floor(Math.random() * relayChannels.length)];
}

// 同时随机选择 bot 与 relay channel。
export async function pickRandomBotAndRelay() {
  const [bot, relay] = await Promise.all([
    pickRandomBot(),
    pickRandomRelayChannel(),
  ]);

  return { bot, relay };
}

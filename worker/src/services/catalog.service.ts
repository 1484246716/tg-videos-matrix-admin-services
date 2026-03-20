import { TaskStatus, CatalogTaskStatus } from '@prisma/client';
import { prisma } from '../infra/prisma';
import {
  editMessageTextByTelegram,
  pinMessageByTelegram,
  sendTextByTelegram,
} from '../shared/telegram';
import { logError } from '../logger';

export async function handleCatalogJob(channelIdRaw: string) {
  let catalogTaskId: bigint | null = null;
  const channelId = BigInt(channelIdRaw);

  const channel = await prisma.channel.findUnique({
    where: { id: channelId },
    include: {
      defaultBot: {
        select: { id: true, status: true, tokenEncrypted: true },
      },
    },
  });

  if (!channel) throw new Error(`未找到频道: ${channelIdRaw}`);
  if (!channel.navEnabled || channel.status !== 'active') {
    return { ok: true, skipped: true, reason: '导航未启用或频道未激活' };
  }
  if (!channel.navTemplateText || !channel.defaultBot) {
    throw new Error(`频道缺少导航模板或默认机器人: ${channelIdRaw}`);
  }
  if (channel.defaultBot.status !== 'active') {
    throw new Error(`频道机器人未启用: ${channelIdRaw}`);
  }

  const bot = channel.defaultBot;

  let catalogTemplate = await prisma.catalogTemplate.findFirst({
    where: { isActive: true },
    select: { id: true },
    orderBy: { createdAt: 'desc' },
  });

  if (!catalogTemplate) {
    catalogTemplate = await prisma.catalogTemplate.create({
      data: {
        name: '默认模板',
        bodyTemplate: '{{#each videos}}\\n{{this.short_title}}\\n{{this.message_url}}\\n{{/each}}',
        recentLimit: 60,
        isActive: true,
      },
      select: { id: true },
    });
  }

  const now = new Date();
  const reuseWindowMs = 2 * 60 * 1000;
  const recentExisting = await prisma.catalogTask.findFirst({
    where: {
      channelId,
      status: { in: [CatalogTaskStatus.pending, CatalogTaskStatus.running] },
    },
    orderBy: { createdAt: 'desc' },
    select: { id: true, createdAt: true },
  });

  if (recentExisting && now.getTime() - recentExisting.createdAt.getTime() <= reuseWindowMs) {
    catalogTaskId = recentExisting.id;
  } else {
    const created = await prisma.catalogTask.create({
      data: {
        channelId,
        catalogTemplateId: catalogTemplate.id,
        status: CatalogTaskStatus.pending,
        plannedAt: now,
        pinAfterPublish: false,
      },
      select: { id: true },
    });
    catalogTaskId = created.id;
  }

  await prisma.catalogTask.update({
    where: { id: catalogTaskId },
    data: { status: CatalogTaskStatus.running, startedAt: now },
  });

  const recentLimit = channel.navRecentLimit ?? 60;
  const dispatchTasks = await prisma.dispatchTask.findMany({
    where: { channelId, status: TaskStatus.success, telegramMessageId: { not: null } },
    orderBy: { finishedAt: 'desc' },
    take: recentLimit,
    select: {
      caption: true,
      telegramMessageLink: true,
    },
  });

  if (dispatchTasks.length === 0) {
    if (catalogTaskId) {
      await prisma.catalogTask.update({
        where: { id: catalogTaskId },
        data: {
          status: CatalogTaskStatus.cancelled,
          finishedAt: new Date(),
          errorMessage: '没有可用的成功分发记录',
        },
      });
    }
    return { ok: true, skipped: true, reason: '没有可用的成功分发记录' };
  }

  const videos = [...dispatchTasks].reverse().map((t) => {
    const parts = (t.caption || '').split('\n').map((l) => l.trim()).filter(Boolean);
    let shortTitle = '未命名视频';
    if (parts.length >= 2) {
      shortTitle = `${parts[0]} ${parts[1]}`;
    } else if (parts.length === 1) {
      shortTitle = parts[0];
    }
    return {
      message_url: t.telegramMessageLink || '',
      short_title: shortTitle,
    };
  });

  let content = channel.navTemplateText;
  content = content.replace(/{{channel_name}}/g, channel.name);
  const eachRegex = /{{#each\s+videos}}([\s\S]*?){{\/each}}/g;
  content = content.replace(eachRegex, (match, body) => {
    return videos
      .map((v) => {
        let text = body.replace(/{{this\.message_url}}/g, v.message_url);
        text = text.replace(/{{this\.short_title}}/g, v.short_title);
        return text;
      })
      .join('');
  });

  const beijingTimeStr = new Date(Date.now() + 8 * 3600 * 1000)
    .toISOString()
    .replace('T', ' ')
    .slice(0, 19);
  content = content.replace(/{{update_time}}/g, beijingTimeStr);

  const botToken = bot.tokenEncrypted;
  let finalMessageId: number | null = null;
  const contentPreview = content.slice(0, 4000);
  let pinAttempted = false;
  let pinSuccess: boolean | null = null;
  let pinErrorMessage: string | null = null;

  try {
    if (channel.navMessageId) {
      const oldMessageId = Number(channel.navMessageId);
      try {
        await editMessageTextByTelegram({
          botToken,
          chatId: channel.tgChatId,
          messageId: oldMessageId,
          text: content,
        });
        finalMessageId = oldMessageId;
      } catch (err) {
        const errObj = err as { message?: string };
        if (
          errObj.message &&
          (errObj.message.includes('message to edit not found') ||
            errObj.message.includes('message is not modified'))
        ) {
          if (errObj.message.includes('not modified')) {
            finalMessageId = oldMessageId;
          } else {
            const sendResult = await sendTextByTelegram({
              botToken,
              chatId: channel.tgChatId,
              text: content,
            });
            finalMessageId = sendResult.messageId;
            pinAttempted = true;
            try {
              await pinMessageByTelegram({
                botToken,
                chatId: channel.tgChatId,
                messageId: finalMessageId,
              });
              pinSuccess = true;
            } catch (pinErr) {
              pinSuccess = false;
              pinErrorMessage = pinErr instanceof Error ? pinErr.message : '置顶失败';
            }
          }
        } else {
          throw err;
        }
      }
    } else {
      const sendResult = await sendTextByTelegram({
        botToken,
        chatId: channel.tgChatId,
        text: content,
      });
      finalMessageId = sendResult.messageId;
      pinAttempted = true;
      try {
        await pinMessageByTelegram({
          botToken,
          chatId: channel.tgChatId,
          messageId: finalMessageId,
        });
        pinSuccess = true;
      } catch (pinErr) {
        pinSuccess = false;
        pinErrorMessage = pinErr instanceof Error ? pinErr.message : '置顶失败';
      }
    }

    await prisma.channel.update({
      where: { id: channelId },
      data: {
        navMessageId: finalMessageId ? BigInt(finalMessageId) : null,
        lastNavUpdateAt: new Date(),
      },
    });

    await prisma.catalogHistory.create({
      data: {
        channelId,
        catalogTemplateId: catalogTemplate.id,
        content,
        renderedCount: videos.length,
        publishedAt: new Date(),
      },
    });

    if (catalogTaskId) {
      await prisma.catalogTask.update({
        where: { id: catalogTaskId },
        data: {
          status: CatalogTaskStatus.success,
          finishedAt: new Date(),
          telegramMessageId: finalMessageId ? BigInt(finalMessageId) : null,
          contentPreview,
          errorMessage: null,
          pinAfterPublish: pinAttempted,
          pinSuccess,
          pinErrorMessage,
        },
      });
    }

    return {
      ok: true,
      channelId: channelIdRaw,
      messageId: finalMessageId,
    };
  } catch (error) {
    if (catalogTaskId) {
      await prisma.catalogTask.update({
        where: { id: catalogTaskId },
        data: {
          status: CatalogTaskStatus.failed,
          finishedAt: new Date(),
          contentPreview,
          errorMessage: error instanceof Error ? error.message : '频道导航发布失败',
          pinAfterPublish: pinAttempted,
          pinSuccess,
          pinErrorMessage:
            pinErrorMessage ?? (error instanceof Error ? error.message : '频道导航发布失败'),
        },
      });
    }

    logError('[q_catalog] 发布频道导航失败', {
      channelId: channelIdRaw,
      error,
    });
    throw error;
  }
}

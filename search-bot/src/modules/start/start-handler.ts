import { verifyDeepLinkToken } from '../deeplink/deeplink.service';
import { injectStartViaMtproto } from '../mtproto/mtproto.client';
import { executeStartPayload } from './start-command-executor';
import { logger } from '../../infra/logger';

export async function handleStartCommand(args: {
  chatId: number;
  fromId?: number;
  text?: string;
  routed: 'message' | 'channel_post';
}) {
  const text = args.text || '';
  const match = text.match(/^\/start\s+cp_(\S+)/);

  logger.info('收到/start命令', {
    chatId: args.chatId,
    fromId: args.fromId,
    routed: args.routed,
    hasPayload: Boolean(match),
  });
  if (!match) {
    return {
      routed: 'start',
      ok: true,
      action: 'send_message',
      send: {
        chatId: args.chatId,
        text: '欢迎使用搜索机器人。请在频道中使用 /s 关键词 进行搜索。',
      },
    };
  }

  // 频道内 /start cp_xxx：直接执行发送
  if (args.routed === 'channel_post') {
    logger.info('频道内/start执行开始', { chatId: args.chatId });

    const result = await executeStartPayload({
      chatId: args.chatId,
      fromId: args.fromId,
      text,
      consumeNonce: true,
      withPrivateAck: false,
    });

    logger.info('频道内/start执行完成', {
      chatId: args.chatId,
      ok: result.ok,
    });

    return {
      routed: 'start',
      ok: result.ok,
      action: 'noop',
    };
  }

  // 私聊 /start cp_xxx：触发 mtproto 注入频道 /start cp_xxx
  const shortToken = match[1];
  const verify = await verifyDeepLinkToken(shortToken, args.fromId ? String(args.fromId) : undefined, {
    consumeNonce: false,
  });

  if (!verify.ok) {
    return {
      routed: 'start',
      ok: true,
      action: 'send_message',
      send: {
        chatId: args.chatId,
        text:
          verify.reason === 'expired'
            ? '链接已过期，请返回频道重新搜索。'
            : verify.reason === 'forbidden'
              ? '你无权触发该发送链接。'
              : verify.reason === 'replayed'
                ? '该链接已使用，请勿重复点击。'
                : '链接无效，请重新获取。',
      },
    };
  }

  const state = verify.state;

  try {
    logger.info('私聊触发注入开始', {
      targetChatId: state.targetChatId,
      shortToken,
    });

    await injectStartViaMtproto({
      chatId: state.targetChatId,
      startPayload: `cp_${shortToken}`,
      deleteDelayMs: 1000,
    });

    logger.info('私聊触发注入完成', {
      targetChatId: state.targetChatId,
      shortToken,
    });

    return {
      routed: 'start',
      ok: true,
      action: 'send_message',
      send: {
        chatId: args.chatId,
        text: '已触发频道发送流程，请稍候查看频道消息。',
      },
    };
  } catch (error) {
    logger.error('私聊触发注入失败', {
      targetChatId: state.targetChatId,
      shortToken,
      error: error instanceof Error ? error.message : String(error),
    });

    return {
      routed: 'start',
      ok: true,
      action: 'send_message',
      send: {
        chatId: args.chatId,
        text: `触发发送失败：${error instanceof Error ? error.message : String(error)}`,
      },
    };
  }
}

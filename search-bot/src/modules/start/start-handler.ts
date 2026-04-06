import { executeStartPayload } from './start-command-executor';
import { logger } from '../../infra/logger';

export async function handleStartCommand(args: {
  chatId: number;
  fromId?: number;
  text?: string;
  routed: 'message';
  triggerMessageId?: number;
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
        text: '欢迎使用搜索机器人。你可以直接发送关键词进行搜索。',
      },
    };
  }

  const result = await executeStartPayload({
    chatId: args.chatId,
    fromId: args.fromId,
    text,
    consumeNonce: false,
    withPrivateAck: args.routed === 'message',
    triggerMessageId: args.triggerMessageId,
    deleteTriggerMessage: args.routed === 'message',
  });

  logger.info('start执行完成', {
    chatId: args.chatId,
    routed: args.routed,
    ok: result.ok,
    message: result.message,
  });

  return {
    routed: 'start',
    ok: true,
    action: 'noop',
  };
}

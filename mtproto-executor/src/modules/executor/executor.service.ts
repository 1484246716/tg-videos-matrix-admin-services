import { Api } from 'telegram';
import { env } from '../../config/env';
import { logger } from '../../infra/logger';
import { getMtprotoClient } from '../../infra/mtproto-client';

export interface InjectStartCommandArgs {
  chatId: string;
  startPayload: string;
  deleteDelayMs?: number;
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function injectStartCommand(args: InjectStartCommandArgs) {
  const deleteDelayMs = args.deleteDelayMs ?? 1000;

  if (env.MTPROTO_DRY_RUN) {
    logger.info('mtproto.inject_start.dry_run', {
      chatId: args.chatId,
      startPayload: args.startPayload,
      deleteDelayMs,
    });

    return {
      ok: true,
      dryRun: true,
      injected: {
        chatId: args.chatId,
        command: `/start ${args.startPayload}`,
      },
      deleted: true,
    };
  }

  const client = await getMtprotoClient();
  const target = await client.getInputEntity(args.chatId);
  const command = `/start ${args.startPayload}`;

  logger.info('mtproto.inject_start.live_begin', {
    chatId: args.chatId,
    deleteDelayMs,
  });

  const sent = await client.sendMessage(target, {
    message: command,
  });

  const sentId = sent.id;
  await sleep(deleteDelayMs);

  await client.invoke(
    new Api.messages.DeleteMessages({
      id: [sentId],
      revoke: true,
    }),
  );

  logger.info('mtproto.inject_start.live_done', {
    chatId: args.chatId,
    sentMessageId: sentId,
    deleted: true,
  });

  return {
    ok: true,
    dryRun: false,
    injected: {
      chatId: args.chatId,
      command,
      messageId: sentId,
    },
    deleted: true,
  };
}

import dotenv from 'dotenv';
import express from 'express';
import { env } from './config/env';
import { checkRedisHealth, markUpdateIdempotent } from './infra/redis';
import { routeTelegramUpdate } from './modules/webhook/telegram-update.router';
import { answerCallbackQuery, editMessage, sendMessage } from './modules/telegram/telegram.client';

dotenv.config();

const app = express();
app.use(express.json());

app.get('/healthz', (_req, res) => {
  res.status(200).json({
    ok: true,
    service: 'search-bot',
    timestamp: new Date().toISOString(),
  });
});

app.get('/readyz', async (_req, res) => {
  const redisOk = await checkRedisHealth();
  if (!redisOk) {
    return res.status(503).json({
      ok: false,
      service: 'search-bot',
      dependency: {
        redis: 'down',
      },
      timestamp: new Date().toISOString(),
    });
  }

  return res.status(200).json({
    ok: true,
    service: 'search-bot',
    dependency: {
      redis: 'up',
    },
    timestamp: new Date().toISOString(),
  });
});

app.post('/telegram/webhook/:secret', async (req, res) => {
  const { secret } = req.params;
  if (secret !== env.BOT_WEBHOOK_SECRET) {
    return res.status(403).json({ ok: false, message: 'forbidden' });
  }

  const update = req.body as { update_id?: number; callback_query?: { id?: string } };
  const updateId = update?.update_id;

  if (typeof updateId !== 'number') {
    return res.status(400).json({ ok: false, message: 'invalid update_id' });
  }

  const firstSeen = await markUpdateIdempotent(updateId);
  if (!firstSeen) {
    return res.status(200).json({ ok: true, deduplicated: true });
  }

  try {
    const routed = await routeTelegramUpdate(req.body);

    if (routed.action === 'send_message' && routed.send) {
      await sendMessage({
        chatId: routed.send.chatId,
        text: routed.send.text,
        replyMarkup: routed.send.replyMarkup,
      });
    }

    if (routed.action === 'edit_message' && routed.edit) {
      await editMessage({
        chatId: routed.edit.chatId,
        messageId: routed.edit.messageId,
        text: routed.edit.text,
        replyMarkup: routed.edit.replyMarkup,
      });
    }

    if (update.callback_query?.id) {
      const notifyText =
        routed.action === 'expired'
          ? '分页已过期，请重新搜索'
          : routed.action === 'forbidden'
            ? '你无权操作此分页'
            : routed.action === 'rate_limited_user'
              ? '你操作过于频繁，请稍后再试'
              : routed.action === 'rate_limited_channel'
                ? '当前频道操作频繁，请稍后再试'
                : undefined;

      await answerCallbackQuery({
        callbackQueryId: update.callback_query.id,
        text: notifyText,
      });
    }

    return res.status(200).json({ ok: true, ...routed });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: 'update handling failed',
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

app.listen(env.SEARCH_BOT_PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`[search-bot] listening on port ${env.SEARCH_BOT_PORT}`);
});

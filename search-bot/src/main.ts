import dotenv from 'dotenv';
import express from 'express';
import { env } from './config/env';
import { checkRedisHealth, markUpdateIdempotent } from './infra/redis';
import { routeTelegramUpdate } from './modules/webhook/telegram-update.router';

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

  const update = req.body as { update_id?: number; message?: unknown; callback_query?: unknown };
  const updateId = update?.update_id;

  if (typeof updateId !== 'number') {
    return res.status(400).json({ ok: false, message: 'invalid update_id' });
  }

  const firstSeen = await markUpdateIdempotent(updateId);
  if (!firstSeen) {
    return res.status(200).json({ ok: true, deduplicated: true });
  }

  try {
    const routed = await routeTelegramUpdate(update);
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

import express from 'express';
import { env } from './config/env';
import { logger } from './infra/logger';
import { postInjectStart } from './modules/executor/executor.controller';
import { checkMtprotoReady } from './infra/mtproto-client';

const app = express();
app.use(express.json());

app.get('/healthz', (_req, res) => {
  res.status(200).json({
    ok: true,
    service: 'mtproto-executor',
    timestamp: new Date().toISOString(),
  });
});

app.get('/readyz', async (_req, res) => {
  const mtprotoOk = await checkMtprotoReady();

  if (!mtprotoOk) {
    return res.status(503).json({
      ok: false,
      service: 'mtproto-executor',
      mode: env.MTPROTO_DRY_RUN ? 'dry-run' : 'live',
      dependency: { mtproto: 'down' },
      timestamp: new Date().toISOString(),
    });
  }

  return res.status(200).json({
    ok: true,
    service: 'mtproto-executor',
    mode: env.MTPROTO_DRY_RUN ? 'dry-run' : 'live',
    dependency: { mtproto: 'up' },
    timestamp: new Date().toISOString(),
  });
});

app.post('/internal/mtproto/inject-start', postInjectStart);

app.listen(env.MTPROTO_EXECUTOR_PORT, () => {
  logger.info('mtproto-executor listening', {
    port: env.MTPROTO_EXECUTOR_PORT,
    mode: env.MTPROTO_DRY_RUN ? 'dry-run' : 'live',
  });
});

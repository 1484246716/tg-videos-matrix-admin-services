import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions';
import { env } from '../config/env';
import { logger } from './logger';

let client: TelegramClient | null = null;
let connecting: Promise<TelegramClient> | null = null;

function createClient() {
  const apiId = Number(env.MTPROTO_API_ID);
  if (!Number.isFinite(apiId) || apiId <= 0) {
    throw new Error('MTPROTO_API_ID must be a positive number in live mode');
  }

  if (!env.MTPROTO_API_HASH || env.MTPROTO_API_HASH === 'dev-api-hash') {
    throw new Error('MTPROTO_API_HASH is not configured for live mode');
  }

  if (!env.MTPROTO_STRING_SESSION || env.MTPROTO_STRING_SESSION === 'dev-string-session') {
    throw new Error('MTPROTO_STRING_SESSION is not configured for live mode');
  }

  return new TelegramClient(new StringSession(env.MTPROTO_STRING_SESSION), apiId, env.MTPROTO_API_HASH, {
    connectionRetries: 5,
    autoReconnect: true,
  });
}

export async function getMtprotoClient(): Promise<TelegramClient> {
  if (client) return client;
  if (connecting) return connecting;

  connecting = (async () => {
    const nextClient = createClient();
    await nextClient.connect();

    const me = await nextClient.getMe();
    logger.info('mtproto.client_connected', {
      userId: me?.id?.toString?.() || null,
      username: (me as { username?: string } | undefined)?.username ?? null,
    });

    client = nextClient;
    connecting = null;
    return nextClient;
  })().catch((error) => {
    connecting = null;
    throw error;
  });

  return connecting;
}

export async function checkMtprotoReady(): Promise<boolean> {
  if (env.MTPROTO_DRY_RUN) return true;

  try {
    const c = await getMtprotoClient();
    await c.getMe();
    return true;
  } catch {
    return false;
  }
}

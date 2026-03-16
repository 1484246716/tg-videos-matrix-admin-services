import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions';
import {
  GRAMJS_API_HASH,
  GRAMJS_API_ID,
  GRAMJS_BOT_TOKEN,
  GRAMJS_SESSION,
} from '../../config/env';

let cachedClient: TelegramClient | null = null;

export async function getGramjsClient() {
  if (cachedClient) return cachedClient;

  if (!GRAMJS_API_ID || !GRAMJS_API_HASH || !GRAMJS_BOT_TOKEN) {
    throw new Error('GramJS 配置缺失：GRAMJS_API_ID / GRAMJS_API_HASH / GRAMJS_BOT_TOKEN');
  }

  const session = new StringSession(GRAMJS_SESSION);
  const client = new TelegramClient(session, GRAMJS_API_ID, GRAMJS_API_HASH, {
    connectionRetries: 5,
  });

  await client.start({
    botAuthToken: GRAMJS_BOT_TOKEN,
  });

  cachedClient = client;
  return client;
}

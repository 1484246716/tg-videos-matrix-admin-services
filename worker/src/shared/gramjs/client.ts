import crypto from 'crypto';
import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions';
import {
  GRAMJS_API_HASH,
  GRAMJS_API_ID,
  GRAMJS_BOT_TOKEN,
  GRAMJS_SESSION,
  GRAMJS_USER_SESSION,
} from '../../config/env';
import { prisma } from '../../infra/prisma';
import { logger } from '../../logger';

let cachedBotClient: TelegramClient | null = null;
let cachedUserClient: TelegramClient | null = null;

function decryptSession(encrypted: string) {
  const keyRaw = process.env.CLONE_ACCOUNT_ENCRYPT_KEY || 'dev-only-insecure-key-change-me';
  const key = crypto.createHash('sha256').update(keyRaw).digest();

  const [ivB64, tagB64, dataB64] = String(encrypted).split('.');
  if (!ivB64 || !tagB64 || !dataB64) {
    throw new Error('invalid encrypted session format');
  }

  const iv = Buffer.from(ivB64, 'base64');
  const tag = Buffer.from(tagB64, 'base64');
  const data = Buffer.from(dataB64, 'base64');

  const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
  decipher.setAuthTag(tag);

  const plain = Buffer.concat([decipher.update(data), decipher.final()]);
  return plain.toString('utf8');
}

async function resolveUserSessionFromDb() {
  const account = await prisma.cloneCrawlAccount.findFirst({
    where: { status: 'active', accountType: 'user' },
    orderBy: { updatedAt: 'desc' },
    select: { sessionString: true, accountPhone: true },
  });

  if (!account?.sessionString) return null;

  const session = decryptSession(account.sessionString);
  if (!session.trim()) return null;

  return {
    session,
    accountPhone: account.accountPhone,
  };
}

export async function getGramjsBotClient() {
  if (cachedBotClient) return cachedBotClient;

  if (!GRAMJS_API_ID || !GRAMJS_API_HASH || !GRAMJS_BOT_TOKEN) {
    throw new Error('GramJS Bot 配置缺失：GRAMJS_API_ID / GRAMJS_API_HASH / GRAMJS_BOT_TOKEN');
  }

  const session = new StringSession('');
  const client = new TelegramClient(session, GRAMJS_API_ID, GRAMJS_API_HASH, {
    connectionRetries: 5,
  });

  await client.start({
    botAuthToken: GRAMJS_BOT_TOKEN,
  });

  cachedBotClient = client;
  return client;
}

export async function getGramjsUserClient() {
  if (cachedUserClient) return cachedUserClient;

  if (!GRAMJS_API_ID || !GRAMJS_API_HASH) {
    throw new Error('GramJS User 配置缺失：GRAMJS_API_ID / GRAMJS_API_HASH');
  }

  let userSession = '';
  let sessionSource: 'db' | 'env' = 'env';

  try {
    const fromDb = await resolveUserSessionFromDb();
    if (fromDb?.session) {
      userSession = fromDb.session;
      sessionSource = 'db';
      logger.info('[gramjs] user session loaded from db', {
        sessionSource,
        accountPhone: fromDb.accountPhone,
      });
    }
  } catch (error) {
    logger.warn('[gramjs] failed to load user session from db, fallback to env', {
      sessionSource: 'env',
      reason: error instanceof Error ? error.message : String(error),
    });
  }

  if (!userSession) {
    userSession = GRAMJS_USER_SESSION || GRAMJS_SESSION;
    if (userSession) {
      logger.info('[gramjs] user session loaded from env', { sessionSource: 'env' });
    }
  }

  if (!userSession) {
    throw new Error('GramJS User 会话缺失：请先手机号登录或配置 GRAMJS_USER_SESSION');
  }

  const session = new StringSession(userSession);
  const client = new TelegramClient(session, GRAMJS_API_ID, GRAMJS_API_HASH, {
    connectionRetries: 5,
  });

  await client.connect();

  if (!await client.isUserAuthorized()) {
    throw new Error(`auth_invalid: GramJS User 会话未授权（sessionSource=${sessionSource}）`);
  }

  logger.info('[gramjs] user client authorized', { sessionSource });

  cachedUserClient = client;
  return client;
}

// 向后兼容：默认返回 user client（clone 抓取场景）
export async function getGramjsClient() {
  return await getGramjsUserClient();
}

/**
 * GramJS 客户端管理：统一维护 Telegram Bot/User 客户端实例。
 * 为 relay / clone-channels 提供可复用的授权会话与连接能力。
 */

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
let cachedUserClientPhone: string | null = null;

// 解密数据库中保存的加密 session 字符串。
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

// 从数据库读取并解密可用 user session 列表。
async function resolveUserSessionsFromDb() {
  const accounts = await prisma.cloneCrawlAccount.findMany({
    where: { status: 'active', accountType: 'user' },
    orderBy: [{ updatedAt: 'desc' }, { id: 'desc' }],
    select: { id: true, sessionString: true, accountPhone: true },
  });

  const result: Array<{ id: string; accountPhone: string; session: string }> = [];

  for (const account of accounts) {
    if (!account?.sessionString) continue;
    try {
      const session = decryptSession(account.sessionString);
      if (!session.trim()) continue;
      result.push({
        id: account.id.toString(),
        accountPhone: account.accountPhone,
        session,
      });
    } catch (error) {
      logger.warn('[gramjs] skip invalid encrypted session in db', {
        accountId: account.id.toString(),
        accountPhone: account.accountPhone,
        reason: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return result;
}

// 获取并缓存 GramJS Bot 客户端。
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

// 获取并缓存 GramJS User 客户端（优先 DB，会话不足时回退 ENV）。
export async function getGramjsUserClient() {
  if (cachedUserClient) {
    try {
      if (await cachedUserClient.isUserAuthorized()) {
        return cachedUserClient;
      }
      logger.warn('[gramjs] cached user client unauthorized, dropping cache', {
        accountPhone: cachedUserClientPhone,
      });
    } catch (error) {
      logger.warn('[gramjs] cached user client check failed, dropping cache', {
        accountPhone: cachedUserClientPhone,
        reason: error instanceof Error ? error.message : String(error),
      });
    }

    try {
      await cachedUserClient.disconnect();
    } catch {
      // ignore
    }
    cachedUserClient = null;
    cachedUserClientPhone = null;
  }

  if (!GRAMJS_API_ID || !GRAMJS_API_HASH) {
    throw new Error('GramJS User 配置缺失：GRAMJS_API_ID / GRAMJS_API_HASH');
  }

  try {
    const candidates = await resolveUserSessionsFromDb();

    for (const candidate of candidates) {
      const session = new StringSession(candidate.session);
      const client = new TelegramClient(session, GRAMJS_API_ID, GRAMJS_API_HASH, {
        connectionRetries: 5,
      });

      try {
        await client.connect();
        const authorized = await client.isUserAuthorized();
        if (!authorized) {
          await prisma.cloneCrawlAccount.update({
            where: { id: BigInt(candidate.id) },
            data: {
              status: 'invalid',
              lastErrorCode: 'AUTH_INVALID',
              lastErrorMessage: 'GramJS session unauthorized',
              lastCheckAt: new Date(),
            },
          });
          await client.disconnect();
          logger.warn('[gramjs] user session unauthorized, auto mark invalid', {
            accountId: candidate.id,
            accountPhone: candidate.accountPhone,
          });
          continue;
        }

        await prisma.cloneCrawlAccount.update({
          where: { id: BigInt(candidate.id) },
          data: {
            status: 'active',
            lastCheckAt: new Date(),
            lastErrorCode: null,
            lastErrorMessage: null,
          },
        });

        cachedUserClient = client;
        cachedUserClientPhone = candidate.accountPhone;

        logger.info('[gramjs] user client authorized', {
          sessionSource: 'db',
          accountId: candidate.id,
          accountPhone: candidate.accountPhone,
        });

        return client;
      } catch (error) {
        await prisma.cloneCrawlAccount.update({
          where: { id: BigInt(candidate.id) },
          data: {
            status: 'invalid',
            lastErrorCode: 'AUTH_INVALID',
            lastErrorMessage: error instanceof Error ? error.message : String(error),
            lastCheckAt: new Date(),
          },
        });

        try {
          await client.disconnect();
        } catch {
          // ignore
        }

        logger.warn('[gramjs] user session failed, auto mark invalid and try next', {
          accountId: candidate.id,
          accountPhone: candidate.accountPhone,
          reason: error instanceof Error ? error.message : String(error),
        });
      }
    }
  } catch (error) {
    logger.warn('[gramjs] failed to load user sessions from db, fallback to env', {
      reason: error instanceof Error ? error.message : String(error),
    });
  }

  const envSession = GRAMJS_USER_SESSION || GRAMJS_SESSION;
  if (envSession) {
    const session = new StringSession(envSession);
    const client = new TelegramClient(session, GRAMJS_API_ID, GRAMJS_API_HASH, {
      connectionRetries: 5,
    });

    await client.connect();

    if (!await client.isUserAuthorized()) {
      throw new Error('auth_invalid: GramJS User 会话未授权（sessionSource=env）');
    }

    cachedUserClient = client;
    cachedUserClientPhone = null;

    logger.info('[gramjs] user client authorized', { sessionSource: 'env' });
    return client;
  }

  throw new Error('GramJS User 会话缺失：请先手机号登录或配置 GRAMJS_USER_SESSION');
}


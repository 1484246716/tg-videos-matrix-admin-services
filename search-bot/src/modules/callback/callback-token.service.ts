import crypto from 'crypto';
import { env } from '../../config/env';
import { getJson, setJsonWithTtl } from '../../infra/redis';

export interface CallbackState {
  keyword: string;
  channelId: string;
  requesterId: string;
  page: number;
  pageSize: number;
}

function buildKey(token: string) {
  return `sb:cb:${token}`;
}

export async function createCallbackToken(state: CallbackState): Promise<string> {
  const token = crypto.randomBytes(8).toString('hex');
  await setJsonWithTtl(buildKey(token), state, env.SEARCH_BOT_CALLBACK_TTL_SEC);
  return token;
}

export async function readCallbackToken(token: string): Promise<CallbackState | null> {
  return getJson<CallbackState>(buildKey(token));
}

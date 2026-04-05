import crypto from 'crypto';
import { env } from '../../config/env';
import { getJson, setIfAbsent, setJsonWithTtl } from '../../infra/redis';

interface DeepLinkState {
  v: 1;
  fromChatId: string;
  messageId: number;
  targetChatId: string;
  exp: number;
  nonce: string;
  requesterId?: string;
  title?: string;
  telegramMessageLink?: string;
  sig: string;
}

function signRaw(raw: string): string {
  return crypto.createHmac('sha256', env.BOT_WEBHOOK_SECRET).update(raw).digest('base64url');
}

function buildRawPayload(state: Omit<DeepLinkState, 'sig'>): string {
  return [
    state.v,
    state.fromChatId,
    state.messageId,
    state.targetChatId,
    state.exp,
    state.nonce,
    state.requesterId || '',
  ].join('|');
}

export async function createDeepLinkToken(args: {
  fromChatId: string;
  messageId: number;
  targetChatId: string;
  requesterId?: string;
  title?: string;
  telegramMessageLink?: string;
}) {
  const shortToken = crypto.randomBytes(9).toString('base64url');
  const nonce = crypto.randomBytes(8).toString('hex');
  const exp = Math.floor(Date.now() / 1000) + env.SEARCH_BOT_DEEPLINK_TTL_SEC;

  const payloadWithoutSig: Omit<DeepLinkState, 'sig'> = {
    v: 1,
    fromChatId: args.fromChatId,
    messageId: args.messageId,
    targetChatId: args.targetChatId,
    exp,
    nonce,
    requesterId: args.requesterId,
    title: args.title,
    telegramMessageLink: args.telegramMessageLink,
  };

  const sig = signRaw(buildRawPayload(payloadWithoutSig));
  const fullState: DeepLinkState = {
    ...payloadWithoutSig,
    sig,
  };

  await setJsonWithTtl(`sb:dl:${shortToken}`, fullState, env.SEARCH_BOT_DEEPLINK_TTL_SEC);
  return shortToken;
}

export function buildDeepLink(shortToken: string): string {
  return `https://t.me/${env.SEARCH_BOT_BOT_USERNAME}?start=cp_${shortToken}`;
}

export async function verifyDeepLinkToken(shortToken: string, requesterId?: string) {
  const state = await getJson<DeepLinkState>(`sb:dl:${shortToken}`);
  if (!state) {
    return { ok: false as const, reason: 'expired' as const };
  }

  const rawSig = signRaw(
    buildRawPayload({
      v: state.v,
      fromChatId: state.fromChatId,
      messageId: state.messageId,
      targetChatId: state.targetChatId,
      exp: state.exp,
      nonce: state.nonce,
      requesterId: state.requesterId,
      title: state.title,
      telegramMessageLink: state.telegramMessageLink,
    }),
  );

  if (rawSig !== state.sig) {
    return { ok: false as const, reason: 'tampered' as const };
  }

  if (Math.floor(Date.now() / 1000) > state.exp) {
    return { ok: false as const, reason: 'expired' as const };
  }

  if (state.requesterId && requesterId && state.requesterId !== requesterId) {
    return { ok: false as const, reason: 'forbidden' as const };
  }

  const nonceFirstSeen = await setIfAbsent(`sb:dl:nonce:${state.nonce}`, '1', env.SEARCH_BOT_DEEPLINK_TTL_SEC);
  if (!nonceFirstSeen) {
    return { ok: false as const, reason: 'replayed' as const };
  }

  return {
    ok: true as const,
    state,
  };
}

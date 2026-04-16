import { cloneGroupL2DownloadQueue, connection } from '../../infra/redis';
import { logger } from '../../logger';
import { CloneMediaDownloadJob } from '../types/clone-queue.types';
import {
  CLONE_GROUP_DISPATCH_TICK_MS,
  CLONE_GROUP_L1_GLOBAL_CONCURRENCY,
  CLONE_GROUP_L1L2_ENABLED,
  CLONE_GROUP_L2_PER_GROUP_CONCURRENCY,
  CLONE_GROUP_ASSEMBLE_TIMEOUT_MS,
  CLONE_USE_LUA_ATOMIC,
} from '../constants/clone-queue.constants';

const L1_RING_KEY = 'clone:l1:rr:ring';
const L1_ACTIVE_SET_KEY = 'clone:l1:active_groups';
const L1_RUNNING_KEY = 'clone:l1:running_count';
const L2_PENDING_PREFIX = 'clone:l2:pending:';
const GROUP_FIRST_SEEN_PREFIX = 'clone:group:first_seen:';

function normalizeGroupKey(job: CloneMediaDownloadJob) {
  if (job.groupKey && job.groupKey.trim()) return job.groupKey.trim();
  if (job.groupedId && job.groupedId.trim()) return `grouped-${job.groupedId.trim()}`;
  return '';
}

function isGroupedCloneJob(job: CloneMediaDownloadJob) {
  return Boolean(normalizeGroupKey(job));
}

function buildPendingKey(groupKey: string) {
  return `${L2_PENDING_PREFIX}${groupKey}`;
}

let hasLoggedLuaAtomicDisabled = false;

function maybeWarnLuaAtomicDisabled() {
  if (CLONE_USE_LUA_ATOMIC || hasLoggedLuaAtomicDisabled) return;
  hasLoggedLuaAtomicDisabled = true;
  logger.warn('[clone][l1l2] CLONE_USE_LUA_ATOMIC is false, fallback path is not implemented; keeping Lua path');
}

export function shouldUseCloneL1L2(job: CloneMediaDownloadJob) {
  if (!CLONE_GROUP_L1L2_ENABLED) return false;
  return isGroupedCloneJob(job);
}

export async function enqueueCloneGroupItem(job: CloneMediaDownloadJob) {
  maybeWarnLuaAtomicDisabled();

  const groupKey = normalizeGroupKey(job);
  if (!groupKey) return false;

  const pendingKey = buildPendingKey(groupKey);
  const payload = {
    ...job,
    groupKey,
    schedulerPath: 'l1l2',
    enqueuedAt: job.enqueuedAt ?? new Date().toISOString(),
  };

  const now = Date.now();
  const lua = `
    local activeSetKey = KEYS[1]
    local ringKey = KEYS[2]
    local pendingKey = KEYS[3]
    local firstSeenKey = KEYS[4]

    local groupKey = ARGV[1]
    local payload = ARGV[2]
    local nowMs = ARGV[3]
    local ttlMs = tonumber(ARGV[4])

    redis.call('RPUSH', pendingKey, payload)

    if redis.call('SISMEMBER', activeSetKey, groupKey) == 0 then
      redis.call('SADD', activeSetKey, groupKey)
      redis.call('RPUSH', ringKey, groupKey)
      redis.call('SET', firstSeenKey, nowMs, 'PX', ttlMs)
      return 'new_group'
    end

    if redis.call('EXISTS', firstSeenKey) == 0 then
      redis.call('SET', firstSeenKey, nowMs, 'PX', ttlMs)
    end

    return 'existing_group'
  `;

  const result = String(
    await connection.eval(
      lua,
      4,
      L1_ACTIVE_SET_KEY,
      L1_RING_KEY,
      pendingKey,
      `${GROUP_FIRST_SEEN_PREFIX}${groupKey}`,
      groupKey,
      JSON.stringify(payload),
      String(now),
      String(Math.max(CLONE_GROUP_ASSEMBLE_TIMEOUT_MS * 2, 60_000)),
    ),
  );

  logger.info('[clone][l1l2] grouped item enqueued', {
    groupKey,
    itemId: job.itemId,
    enqueueResult: result,
  });

  return true;
}

export async function dispatchCloneGroupOneRound() {
  maybeWarnLuaAtomicDisabled();

  const lua = `
    local ringKey = KEYS[1]
    local activeSetKey = KEYS[2]
    local runningKey = KEYS[3]
    local pendingPrefix = ARGV[1]
    local globalLimit = tonumber(ARGV[2])

    local runningRaw = redis.call('GET', runningKey)
    local running = tonumber(runningRaw)
    if running == nil then running = 0 end

    if running >= globalLimit then
      return cjson.encode({status='global_busy'})
    end

    local ringSize = redis.call('LLEN', ringKey)
    if ringSize <= 0 then
      return cjson.encode({status='empty'})
    end

    for i=1,ringSize do
      local gk = redis.call('LPOP', ringKey)
      if not gk then
        return cjson.encode({status='empty'})
      end

      local pendingKey = pendingPrefix .. gk
      local payload = redis.call('LPOP', pendingKey)
      local remaining = redis.call('LLEN', pendingKey)

      if payload then
        if remaining > 0 then
          redis.call('RPUSH', ringKey, gk)
        else
          redis.call('SREM', activeSetKey, gk)
        end

        redis.call('INCR', runningKey)
        return cjson.encode({status='dispatched', groupKey=gk, payload=payload, remaining=remaining})
      else
        redis.call('SREM', activeSetKey, gk)
      end
    end

    return cjson.encode({status='no_payload'})
  `;

  const raw = String(
    await connection.eval(
      lua,
      3,
      L1_RING_KEY,
      L1_ACTIVE_SET_KEY,
      L1_RUNNING_KEY,
      L2_PENDING_PREFIX,
      String(Math.max(1, CLONE_GROUP_L1_GLOBAL_CONCURRENCY)),
    ),
  );

  const parsed = JSON.parse(raw) as {
    status: 'global_busy' | 'empty' | 'dispatched' | 'no_payload';
    groupKey?: string;
    payload?: string;
    remaining?: number;
  };

  if (parsed.status !== 'dispatched' || !parsed.payload) {
    logger.info('[clone][l1l2] l1 tick no-dispatch', {
      status: parsed.status,
      globalConcurrency: CLONE_GROUP_L1_GLOBAL_CONCURRENCY,
    });
    return false;
  }

  const payload = JSON.parse(parsed.payload) as CloneMediaDownloadJob;

  await cloneGroupL2DownloadQueue.add(
    'clone-group-l2-download',
    payload,
    {
      removeOnComplete: true,
      removeOnFail: 100,
      delay: Math.max(0, CLONE_GROUP_DISPATCH_TICK_MS),
    },
  );

  logger.info('[clone][l1l2] l1 dispatched group item', {
    groupKey: parsed.groupKey,
    itemId: payload.itemId,
    remaining: parsed.remaining,
  });

  return true;
}

export async function onCloneGroupL2Done() {
  await connection.decr(L1_RUNNING_KEY);
}

export function getCloneGroupL2Concurrency() {
  return Math.max(1, CLONE_GROUP_L2_PER_GROUP_CONCURRENCY);
}

/**
 * Redis 与 BullMQ 基础设施初始化：提供 worker 侧连接与队列实例。
 * 为 bootstrap、scheduler、service 与 worker 提供统一队列访问。
 */

import IORedis from 'ioredis';
import { Queue } from 'bullmq';
import { redisUrl } from '../config/env';

export const connection = new IORedis(redisUrl, { maxRetriesPerRequest: null });

connection.on('error', (error) => {
  console.error('[redis] worker connection error:', error?.message ?? error);
});

export const dispatchQueue = new Queue('q_dispatch', {
  connection: connection as any,
});
export const catalogQueue = new Queue('q_catalog', {
  connection: connection as any,
});
export const relayUploadQueue = new Queue('q_relay_upload', {
  connection: connection as any,
});

export const massMessageQueue = new Queue('q_mass_message', {
  connection: connection as any,
});
export const backfillQueue = new Queue('q_relay_fileid_backfill', {
  connection: connection as any,
});
export const searchIndexQueue = new Queue('q_search_index', {
  connection: connection as any,
});
export const collectionSnapshotQueue = new Queue('q_collection_snapshot', {
  connection: connection as any,
});

export const cloneCrawlScheduleQueue = new Queue('q_clone_crawl_schedule', {
  connection: connection as any,
});

export const cloneChannelIndexQueue = new Queue('q_clone_channel_index', {
  connection: connection as any,
});

export const cloneMediaDownloadQueue = new Queue('q_clone_media_download', {
  connection: connection as any,
});

export const cloneGroupL1DispatchQueue = new Queue('q_clone_group_l1_dispatch', {
  connection: connection as any,
});

export const cloneGroupL2DownloadQueue = new Queue('q_clone_group_l2_download', {
  connection: connection as any,
});

export const cloneGuardWaitQueue = new Queue('q_clone_download_guard_wait', {
  connection: connection as any,
});

export const cloneRetryQueue = new Queue('q_clone_retry', {
  connection: connection as any,
});

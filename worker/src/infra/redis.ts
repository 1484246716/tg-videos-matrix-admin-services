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

export const cloneVideoDownloadQueue = new Queue('q_clone_video_download', {
  connection: connection as any,
});

export const cloneRetryQueue = new Queue('q_clone_retry', {
  connection: connection as any,
});

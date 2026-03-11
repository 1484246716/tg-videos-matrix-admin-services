import IORedis from 'ioredis';
import { Queue } from 'bullmq';
import { redisUrl } from '../config/env';

export const connection = new IORedis(redisUrl, { maxRetriesPerRequest: null });

export const dispatchQueue = new Queue('q_dispatch', {
  connection: connection as any,
});
export const catalogQueue = new Queue('q_catalog', {
  connection: connection as any,
});
export const relayUploadQueue = new Queue('q_relay_upload', {
  connection: connection as any,
});

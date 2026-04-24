/**
 * Clone Channels 任务队列与服务层共享类型定义。
 * 为 clone scheduler、worker 与 service 提供统一的入参/状态类型约束。
 */

export type CloneCrawlScheduleJob = {
  taskId: string;
  runAt: string;
};

export type CloneContentType = 'text' | 'image' | 'video';

export type CloneChannelIndexJob = {
  taskId: string;
  channelUsername: string;
  runId: string;
  channelId?: string;
  lastFetchedMessageId?: string;
  recentLimit?: number;
  contentTypes?: CloneContentType[];
  enqueuedAt?: string;
  retryCount?: number;
};

export type CloneMediaDownloadPriority = 'small' | 'medium' | 'large';

export type CloneMediaRef =
  | { kind: 'tg_message'; channelUsername: string; messageId: string }
  | { kind: 'local_file'; filePath: string }
  | { kind: 'opaque'; value: string };

export type CloneMediaDownloadJob = {
  taskId: string;
  runId: string;
  itemId: string;
  channelUsername?: string;
  groupedId?: string;
  groupKey?: string;
  mediaRef?: CloneMediaRef;
  expectedFileSize?: string;
  expectedMimeType?: string;
  expectedFileName?: string;
  targetPath?: string;
  priority?: CloneMediaDownloadPriority;
  enqueuedAt?: string;
  retryCount?: number;
};

export type CloneRetryQueue = 'index' | 'download';

export type CloneRetryReason =
  | 'flood_wait'
  | 'network_timeout'
  | 'auth_invalid'
  | 'channel_unreachable'
  | 'file_too_large'
  | 'disk_guard_triggered'
  | 'index_unknown_error'
  | 'download_unknown_error'
  | 'retry_exhausted';

export type CloneRetryJob = {
  queue: CloneRetryQueue;
  payload: CloneChannelIndexJob | CloneMediaDownloadJob | Record<string, unknown>;
  reason: CloneRetryReason | string;
  retryCount?: number;
  retryAfterSec?: number;
  nonRetryable?: boolean;
  firstFailedAt?: string;
  lastErrorMessage?: string;
};

export type IndexedMessageDTO = {
  messageId: bigint;
  groupedId?: string;
  groupKey?: string;
  messageDate?: Date;
  messageText?: string;
  hasVideo: boolean;
  fileSize?: bigint;
  mimeType?: string;
  mediaRef?: string;
};

export type GuardReason =
  | 'disk_guard_triggered'
  | 'inflight_budget_exceeded'
  | 'global_concurrency_exceeded'
  | 'per_channel_concurrency_exceeded';

export type GuardDecision =
  | { pass: true; diskUsagePercent: number }
  | { pass: false; reason: GuardReason; retryDelayMs: number };

export type ResourceSnapshot = {
  diskUsagePercent: number;
  inflightBytes: bigint;
  globalDownloadingCount: number;
  channelDownloadingCount: number;
};

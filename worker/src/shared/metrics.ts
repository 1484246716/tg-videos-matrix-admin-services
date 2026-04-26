/**
 * 指标计数与常量定义：供 scheduler/worker/service 共享使用。
 * 统一沉淀运行计数器、告警阈值标签与错误原因枚举。
 */

export const taskdefMetrics = {
  tickTotal: 0,
  dueTotal: 0,
  lockSkipTotal: 0,
  runSuccessTotal: 0,
  runFailedTotal: 0,
  runDurationMsTotal: 0,
};

export const catalogMetrics = {
  tickTotal: 0,
  publishRunTotal: 0,
  publishRunSchedulerTotal: 0,
  publishRunManualRepairTotal: 0,
  publishRunSuccessTotal: 0,
  publishRunFailedTotal: 0,
  publishRunSkippedTotal: 0,
  publishEmptyRunTotal: 0,
  publishItemsRenderedTotal: 0,
  publishGroupItemsRenderedTotal: 0,
  publishCollectionItemsRenderedTotal: 0,
  publishDurationMsTotal: 0,
  publishEmptyRunConsecutive: 0,
  manualRepairTriggeredTotal: 0,
  manualRepairAlreadyInProgressTotal: 0,
  hashGateTotal: 0,
  hashGateSkipTotal: 0,
  hashGatePublishTotal: 0,
};

export const catalogSourceWriteMetrics = {
  upsertTotal: 0,
  upsertSuccessTotal: 0,
  upsertFailedTotal: 0,
  upsertSingleTotal: 0,
  upsertGroupTotal: 0,
  skippedCollectionTotal: 0,
  deletedCollectionTotal: 0,
  dedupHitTotal: 0,
  upsertDurationMsTotal: 0,
};

export const METRICS_LOG_INTERVAL_TICKS = 60;

export const TYPEA_INGEST_ERROR_CODE = {
  srcFileMissing: 'SRC_FILE_MISSING',
  ingestRuntimeError: 'INGEST_RUNTIME_ERROR',
  ingestStuckTimeout: 'INGEST_STUCK_TIMEOUT',
  fileTooLarge: 'FILE_TOO_LARGE',
} as const;

export const TYPEA_INGEST_FINAL_REASON = {
  failedFinal: 'FAILED_FINAL',
  retryable: 'RETRYABLE',
  staleIngestingExceeded: 'STALE_INGESTING_EXCEEDED',
  fileTooLarge: 'FILE_TOO_LARGE',
} as const;

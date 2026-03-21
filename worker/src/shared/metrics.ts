export const taskdefMetrics = {
  tickTotal: 0,
  dueTotal: 0,
  lockSkipTotal: 0,
  runSuccessTotal: 0,
  runFailedTotal: 0,
  runDurationMsTotal: 0,
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
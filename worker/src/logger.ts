import winston from 'winston';
import DailyRotateFile from 'winston-daily-rotate-file';
import { mkdirSync } from 'node:fs';
import { resolve } from 'node:path';

const LOG_DIR = process.env.WORKER_LOG_DIR || resolve(process.cwd(), 'logs');
const CLONE_LOG_DIR = resolve(LOG_DIR, 'clone-channels');
const LOG_LEVEL = process.env.WORKER_LOG_LEVEL || 'info';
const LOG_RETENTION_DAYS = Number(process.env.WORKER_LOG_RETENTION_DAYS || '14');

mkdirSync(LOG_DIR, { recursive: true });
mkdirSync(CLONE_LOG_DIR, { recursive: true });

function isCloneLog(info: winston.Logform.TransformableInfo) {
  const message = String(info.message ?? '');
  return message.includes('[clone]') || message.includes('[clone-');
}

const onlyCloneLogs = winston.format((info) => (isCloneLog(info) ? info : false));
const excludeCloneLogs = winston.format((info) => (isCloneLog(info) ? false : info));

const fileTransport = new DailyRotateFile({
  dirname: LOG_DIR,
  filename: 'worker-%DATE%.log',
  datePattern: 'YYYY-MM-DD',
  zippedArchive: false,
  maxFiles: `${LOG_RETENTION_DAYS}d`,
  level: LOG_LEVEL,
  format: excludeCloneLogs(),
});

const cloneFileTransport = new DailyRotateFile({
  dirname: CLONE_LOG_DIR,
  filename: 'clone-worker-%DATE%.log',
  datePattern: 'YYYY-MM-DD',
  zippedArchive: false,
  maxFiles: `${LOG_RETENTION_DAYS}d`,
  level: LOG_LEVEL,
  format: onlyCloneLogs(),
});

const consoleTransport = new winston.transports.Console({
  level: LOG_LEVEL,
});

export const logger = winston.createLogger({
  level: LOG_LEVEL,
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json(),
  ),
  transports: [fileTransport, cloneFileTransport, consoleTransport],
});

export function toReadableErrorSummary(error: unknown): string {
  if (error instanceof Error) {
    return error.message || error.name || 'Unknown error';
  }

  if (typeof error === 'string') {
    return error;
  }

  if (typeof error === 'number' || typeof error === 'boolean' || error === null || error === undefined) {
    return String(error);
  }

  if (typeof error === 'object') {
    const errObj = error as {
      message?: unknown;
      code?: unknown;
      name?: unknown;
      error?: unknown;
    };

    if (typeof errObj.message === 'string' && errObj.message.trim()) {
      return errObj.message;
    }

    if (typeof errObj.error === 'string' && errObj.error.trim()) {
      return errObj.error;
    }

    try {
      const json = JSON.stringify(error);
      if (json && json !== '{}') {
        return json;
      }
    } catch {
      // ignore
    }

    if (typeof errObj.code === 'string' && errObj.code.trim()) {
      return `Error code: ${errObj.code}`;
    }

    if (typeof errObj.name === 'string' && errObj.name.trim()) {
      return errObj.name;
    }
  }

  return 'Unknown error';
}

export function logError(message: string, error?: unknown) {
  if (!error) {
    logger.error(message);
    return;
  }

  if (error instanceof Error) {
    logger.error(message, { error: { name: error.name, message: error.message, stack: error.stack } });
    return;
  }

  logger.error(message, { error, errorSummary: toReadableErrorSummary(error) });
}

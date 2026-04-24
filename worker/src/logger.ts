/**
 * Worker 日志模块：统一输出普通日志与 clone 专用日志。
 * 为 scheduler / worker / service / shared 提供一致的日志与错误记录能力。
 */

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

// 判断是否为 clone 相关日志。
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

// 将未知错误转换为可读摘要。
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

// 序列化未知错误对象，便于结构化日志落盘。
function serializeUnknownError(error: unknown): Record<string, unknown> {
  if (error instanceof Error) {
    const withCause = error as Error & { cause?: unknown };
    return {
      type: 'Error',
      name: error.name,
      message: error.message,
      stack: error.stack,
      cause:
        withCause.cause === undefined
          ? undefined
          : withCause.cause instanceof Error
            ? {
                name: withCause.cause.name,
                message: withCause.cause.message,
                stack: withCause.cause.stack,
              }
            : withCause.cause,
    };
  }

  if (typeof error === 'object' && error !== null) {
    const obj = error as Record<string, unknown>;
    return {
      type: obj?.constructor?.name ?? 'Object',
      ...obj,
      json: (() => {
        try {
          return JSON.stringify(error);
        } catch {
          return '[unserializable-object]';
        }
      })(),
    };
  }

  return {
    type: typeof error,
    value: error,
  };
}

// 统一错误日志入口。
export function logError(message: string, error?: unknown) {
  if (!error) {
    logger.error(message);
    return;
  }

  logger.error(message, {
    error: serializeUnknownError(error),
    errorSummary: toReadableErrorSummary(error),
  });
}

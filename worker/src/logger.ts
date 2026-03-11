import winston from 'winston';
import DailyRotateFile from 'winston-daily-rotate-file';
import { mkdirSync } from 'node:fs';
import { resolve } from 'node:path';

const LOG_DIR = process.env.WORKER_LOG_DIR || resolve(process.cwd(), 'logs');
const LOG_LEVEL = process.env.WORKER_LOG_LEVEL || 'info';
const LOG_RETENTION_DAYS = Number(process.env.WORKER_LOG_RETENTION_DAYS || '14');

mkdirSync(LOG_DIR, { recursive: true });

const fileTransport = new DailyRotateFile({
  dirname: LOG_DIR,
  filename: 'worker-%DATE%.log',
  datePattern: 'YYYY-MM-DD',
  zippedArchive: false,
  maxFiles: `${LOG_RETENTION_DAYS}d`,
  level: LOG_LEVEL,
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
  transports: [fileTransport, consoleTransport],
});

export function logError(message: string, error?: unknown) {
  if (!error) {
    logger.error(message);
    return;
  }

  if (error instanceof Error) {
    logger.error(message, { error: { name: error.name, message: error.message, stack: error.stack } });
    return;
  }

  logger.error(message, { error });
}

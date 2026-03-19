import { LoggerService } from '@nestjs/common';
import winston = require('winston');
import DailyRotateFile = require('winston-daily-rotate-file');
import { mkdirSync } from 'node:fs';
import { resolve } from 'node:path';

const LOG_DIR = process.env.API_LOG_DIR || resolve(process.cwd(), 'logs');
const LOG_LEVEL = process.env.API_LOG_LEVEL || 'info';
const LOG_RETENTION_DAYS = Number(process.env.API_LOG_RETENTION_DAYS || '14');

mkdirSync(LOG_DIR, { recursive: true });

const fileTransport = new DailyRotateFile({
  dirname: LOG_DIR,
  filename: 'api-%DATE%.log',
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

export class AppLogger implements LoggerService {
  log(message: string, context?: string) {
    logger.info(message, { context });
  }

  error(message: string, trace?: string, context?: string) {
    logger.error(message, { trace, context });
  }

  warn(message: string, context?: string) {
    logger.warn(message, { context });
  }

  debug(message: string, context?: string) {
    logger.debug(message, { context });
  }

  verbose(message: string, context?: string) {
    logger.verbose(message, { context });
  }
}

import fs from 'fs';
import path from 'path';

type LogLevel = 'debug' | 'info' | 'warn' | 'error';

const LOG_DIR = path.resolve(process.cwd(), 'search-bot', 'logs');
const LOG_FILE = path.join(LOG_DIR, 'search-bot.log');

function shouldLog(level: LogLevel) {
  const envLevel = (process.env.SEARCH_BOT_LOG_LEVEL || 'info').toLowerCase();
  const order: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };
  const current = (order as Record<string, number>)[envLevel] ?? 20;
  return order[level] >= current;
}

function ensureLogFileReady() {
  try {
    if (!fs.existsSync(LOG_DIR)) {
      fs.mkdirSync(LOG_DIR, { recursive: true });
    }
    if (!fs.existsSync(LOG_FILE)) {
      fs.writeFileSync(LOG_FILE, '', 'utf8');
    }
  } catch {
    // ignore file logger setup errors to avoid breaking main flow
  }
}

function writeToFile(line: string) {
  try {
    ensureLogFileReady();
    fs.appendFileSync(LOG_FILE, `${line}\n`, 'utf8');
  } catch {
    // ignore file write errors to avoid breaking main flow
  }
}

function write(level: LogLevel, message: string, meta?: Record<string, unknown>) {
  if (!shouldLog(level)) return;
  const payload = {
    ts: new Date().toISOString(),
    level,
    service: 'search-bot',
    message,
    ...(meta ? { meta } : {}),
  };

  const line = JSON.stringify(payload);

  // eslint-disable-next-line no-console
  console.log(line);
  writeToFile(line);
}

export const logger = {
  debug(message: string, meta?: Record<string, unknown>) {
    write('debug', message, meta);
  },
  info(message: string, meta?: Record<string, unknown>) {
    write('info', message, meta);
  },
  warn(message: string, meta?: Record<string, unknown>) {
    write('warn', message, meta);
  },
  error(message: string, meta?: Record<string, unknown>) {
    write('error', message, meta);
  },
};

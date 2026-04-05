type LogLevel = 'debug' | 'info' | 'warn' | 'error';

function shouldLog(level: LogLevel) {
  const envLevel = (process.env.SEARCH_BOT_LOG_LEVEL || 'info').toLowerCase();
  const order: Record<LogLevel, number> = { debug: 10, info: 20, warn: 30, error: 40 };
  const current = (order as Record<string, number>)[envLevel] ?? 20;
  return order[level] >= current;
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
  // eslint-disable-next-line no-console
  console.log(JSON.stringify(payload));
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

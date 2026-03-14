import { bootstrapWorker } from './bootstrap/bootstrap';
import { logError } from './logger';

bootstrapWorker().catch((err) => {
  logError('Worker 启动失败', err);
  process.exit(1);
});

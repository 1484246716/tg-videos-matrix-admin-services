/**
 * Worker 进程主入口：启动 bootstrap 初始化流程。
 * 从 Node 启动点进入 main，并调用 bootstrapWorker 完成组件装配。
 */

import { bootstrapWorker } from './bootstrap/bootstrap';
import { logError } from './logger';

bootstrapWorker().catch((err) => {
  logError('Worker 启动失败', err);
  process.exit(1);
});

import { bootstrapWorker } from './bootstrap/bootstrap';
import { logError } from './logger';

bootstrapWorker().catch((err) => {
  logError('Worker bootstrap error', err);
  process.exit(1);
});

import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { SearchIndexerService } from './search-indexer.service';

@Injectable()
export class SearchOutboxScheduler implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(SearchOutboxScheduler.name);
  private timer: NodeJS.Timeout | null = null;
  private running = false;

  constructor(private readonly searchIndexerService: SearchIndexerService) {}

  onModuleInit() {
    const enabled = process.env.SEARCH_OUTBOX_SCHEDULER_ENABLED !== 'false';
    if (!enabled) {
      this.logger.warn('[search-outbox-scheduler] 已禁用');
      return;
    }

    const intervalMs = this.readNumber('SEARCH_OUTBOX_SCHEDULER_INTERVAL_MS', 5000, 1000, 60000);
    this.timer = setInterval(() => {
      void this.tick();
    }, intervalMs);

    this.logger.log(`[search-outbox-scheduler] 已启动，interval=${intervalMs}ms`);
    void this.tick();
  }

  onModuleDestroy() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private async tick() {
    if (this.running) return;
    this.running = true;

    try {
      const limit = this.readNumber('SEARCH_OUTBOX_BATCH_LIMIT', 100, 1, 1000);
      const result = await this.searchIndexerService.processBatch(limit);
      if (result.processed > 0) {
        this.logger.log(
          `[search-outbox-scheduler] processed=${result.processed}, success=${result.success}, failed=${result.failed}`,
        );
      }
    } catch (error) {
      this.logger.error(
        `[search-outbox-scheduler] tick error: ${error instanceof Error ? error.message : String(error)}`,
      );
    } finally {
      this.running = false;
    }
  }

  private readNumber(key: string, fallback: number, min: number, max: number) {
    const raw = process.env[key];
    const value = raw ? Number.parseInt(raw, 10) : fallback;
    if (!Number.isFinite(value)) return fallback;
    return Math.min(max, Math.max(min, value));
  }
}

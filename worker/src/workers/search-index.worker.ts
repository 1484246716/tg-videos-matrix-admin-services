/**
 * Search Index Worker：消费搜索索引队列并执行文档构建/更新/删除。
 * 在 bootstrap 注册后由 BullMQ 驱动，负责接收 job 并调用 search-index service。
 */

/**
 * search-index.worker.ts
 * ──────────────────────
 * BullMQ Worker：q_search_index 队列消费者
 * 处理搜索文档的构建/更新/删除任务
 */
import { Worker } from 'bullmq';
import { connection } from '../infra/redis';
import { logger, logError } from '../logger';
import { handleSearchIndexJob, type SearchIndexJobData } from '../services/search-index.service';

export const searchIndexWorker = new Worker(
  'q_search_index',
  async (job) => {
    if (job.name === 'bootstrap-check') {
      return { ok: true, skipped: true, reason: 'bootstrap-check' };
    }

    const data = job.data as SearchIndexJobData;
    if (!data.sourceType || !data.sourceId) {
      throw new Error('搜索索引任务缺少 sourceType 或 sourceId');
    }

    await handleSearchIndexJob(data);

    return { ok: true, docId: data.sourceId, sourceType: data.sourceType };
  },
  {
    connection: connection as any,
    concurrency: 3,
    limiter: {
      max: 50,
      duration: 1000,
    },
  },
);

searchIndexWorker.on('completed', (job) => {
  logger.info('[q_search_index] 任务完成', { jobId: String(job.id) });
});

searchIndexWorker.on('failed', (job, err) => {
  logError('[q_search_index] 任务失败', {
    jobId: job?.id ? String(job.id) : null,
    error: err,
  });
});

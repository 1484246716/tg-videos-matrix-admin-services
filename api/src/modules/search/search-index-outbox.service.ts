import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

export type SearchOutboxOp = 'upsert' | 'delete';

@Injectable()
export class SearchIndexOutboxService {
  private readonly logger = new Logger(SearchIndexOutboxService.name);

  constructor(private readonly prisma: PrismaService) {}

  async enqueue(docId: string, op: SearchOutboxOp, payload?: unknown) {
    return this.prisma.searchIndexOutbox.create({
      data: {
        docId,
        op,
        payload: payload ? (payload as object) : undefined,
        status: 'pending',
        attempt: 0,
        nextRetryAt: null,
        lastError: null,
      },
    });
  }

  async claimPendingBatch(limit = 100) {
    return this.prisma.$transaction(async (tx) => {
      const pendingRows = await tx.$queryRaw<Array<{ id: bigint }>>`
        SELECT id
        FROM search_index_outbox
        WHERE status = 'pending'
          AND (next_retry_at IS NULL OR next_retry_at <= NOW())
        ORDER BY created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT ${limit}
      `;

      if (pendingRows.length === 0) return [];
      const ids = pendingRows.map((row) => row.id);

      await tx.searchIndexOutbox.updateMany({
        where: {
          id: { in: ids },
        },
        data: {
          status: 'processing',
          updatedAt: new Date(),
        },
      });

      return tx.searchIndexOutbox.findMany({
        where: { id: { in: ids } },
        orderBy: { createdAt: 'asc' },
      });
    });
  }

  async markDone(id: bigint) {
    await this.prisma.searchIndexOutbox.update({
      where: { id },
      data: {
        status: 'done',
        updatedAt: new Date(),
      },
    });
  }

  async markFailed(id: bigint, currentAttempt: number, error: unknown) {
    const attempt = currentAttempt + 1;
    const terminal = attempt >= 8;
    const delaySec = terminal ? null : Math.min(300, 2 ** attempt);

    await this.prisma.searchIndexOutbox.update({
      where: { id },
      data: {
        status: terminal ? 'failed' : 'pending',
        attempt,
        lastError: this.toErrorText(error),
        nextRetryAt: terminal || delaySec === null ? null : new Date(Date.now() + delaySec * 1000),
        updatedAt: new Date(),
      },
    });

    if (terminal) {
      this.logger.error(`outbox message reached terminal failure: ${id.toString()}`);
    }
  }

  private toErrorText(error: unknown): string {
    if (error instanceof Error) return `${error.name}: ${error.message}`.slice(0, 2000);
    return String(error).slice(0, 2000);
  }
}

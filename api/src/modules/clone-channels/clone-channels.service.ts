import {
  BadRequestException,
  Injectable,
  InternalServerErrorException,
  NotFoundException,
  Logger,
} from '@nestjs/common';
import * as crypto from 'node:crypto';
import { StringSession } from 'telegram/sessions';
import { TelegramClient } from 'telegram';
import { Api } from 'telegram';
import { computeCheck } from 'telegram/Password';
import { CloneDownloadStatus } from '@prisma/client';
import IORedis from 'ioredis';
import { Queue } from 'bullmq';
import { PrismaService } from '../prisma/prisma.service';
import { CreateCloneTaskDto } from './dto/create-clone-task.dto';
import { UpdateCloneTaskDto } from './dto/update-clone-task.dto';

@Injectable()
export class CloneChannelsService {
  private readonly logger = new Logger(CloneChannelsService.name);
  private readonly redis = new IORedis(process.env.REDIS_URL || 'redis://localhost:6379', {
    maxRetriesPerRequest: null,
  });
  private readonly cloneMediaDownloadQueue = new Queue('q_clone_media_download', {
    connection: this.redis as any,
  });
  private readonly cloneGuardWaitQueue = new Queue('q_clone_download_guard_wait', {
    connection: this.redis as any,
  });
  private readonly cloneRetryQueue = new Queue('q_clone_retry', {
    connection: this.redis as any,
  });

  private readonly loginFlowStore = new Map<string, {
    phoneCodeHashFromTg: string;
    tempSession: string;
    createdAt: number;
  }>();

  constructor(private readonly prisma: PrismaService) {}

  private getGramjsBaseConfig() {
    const apiId = Number(process.env.GRAMJS_API_ID ?? '0');
    const apiHash = String(process.env.GRAMJS_API_HASH ?? '').trim();

    if (!Number.isFinite(apiId) || apiId <= 0 || !apiHash) {
      throw new BadRequestException('GRAMJS_API_ID / GRAMJS_API_HASH 未配置，无法执行手机号登录');
    }

    return { apiId, apiHash };
  }

  private async withTempTelegramClient<T>(
    fn: (client: TelegramClient) => Promise<T>,
    initialSession = '',
  ): Promise<T> {
    const { apiId, apiHash } = this.getGramjsBaseConfig();
    const session = new StringSession(initialSession);
    const client = new TelegramClient(session, apiId, apiHash, {
      connectionRetries: 2,
    });

    await client.connect();
    try {
      return await fn(client);
    } finally {
      await client.disconnect();
    }
  }

  private encryptSession(session: string) {
    const keyRaw = process.env.CLONE_ACCOUNT_ENCRYPT_KEY || 'dev-only-insecure-key-change-me';
    const key = crypto.createHash('sha256').update(keyRaw).digest();
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
    const encrypted = Buffer.concat([cipher.update(session, 'utf8'), cipher.final()]);
    const tag = cipher.getAuthTag();
    return `${iv.toString('base64')}.${tag.toString('base64')}.${encrypted.toString('base64')}`;
  }

  private async assertHasActiveCrawlAccount() {
    const envSession = process.env.GRAMJS_USER_SESSION || process.env.GRAMJS_SESSION;
    if (envSession && envSession.trim()) return;

    const active = await this.prisma.cloneCrawlAccount.findFirst({
      where: { status: 'active', accountType: 'user' },
      select: { id: true },
    });

    if (!active) {
      throw new BadRequestException('NO_ACTIVE_CRAWL_ACCOUNT: 请先配置并登录用户账号会话（GRAMJS_USER_SESSION）');
    }
  }

  private serializeBigInt<T>(value: T): T {
    return JSON.parse(
      JSON.stringify(value, (_key, v) =>
        typeof v === 'bigint' ? v.toString() : v,
      ),
    ) as T;
  }

  private get taskModel() {
    const model = this.prisma.cloneCrawlTask;
    if (!model) {
      throw new InternalServerErrorException(
        'Prisma cloneCrawlTask model is unavailable. Please run prisma generate and restart API.',
      );
    }

    return model;
  }

  private normalizeChannels(channels: string[]) {
    const normalized = channels
      .map((item) => item.trim())
      .filter((item) => item.length > 0)
      .map((item) => {
        const fromUrl = item.match(/^https?:\/\/t\.me\/([a-zA-Z0-9_]{5,})/i);
        if (fromUrl) return `@${fromUrl[1]}`;
        if (item.startsWith('@')) return item;
        return `@${item}`;
      });

    return Array.from(new Set(normalized));
  }

  private parseSingleMessageLink(raw?: string | null) {
    const value = String(raw ?? '').trim();
    if (!value) return null;

    const matched = value.match(
      /^(?:https?:\/\/)?(?:www\.)?t\.me\/([a-zA-Z0-9_]{5,})\/(\d+)(?:[/?#].*)?$/i,
    );

    if (!matched) {
      throw new BadRequestException(
        'singleMessageLink must be a Telegram message link like https://t.me/channel/123',
      );
    }

    const channelUsername = `@${matched[1].toLowerCase()}`;
    const messageId = BigInt(matched[2]);
    if (messageId <= 0n) {
      throw new BadRequestException('singleMessageLink message id must be greater than 0');
    }

    return {
      normalizedLink: `https://t.me/${matched[1]}/${matched[2]}`,
      channelUsername,
      messageId,
    };
  }

  private parseSingleMessageLinks(rawLinks?: string[] | null, fallbackRawLink?: string | null) {
    const list = Array.isArray(rawLinks) ? rawLinks : [];
    const candidates = list.length > 0 ? list : (fallbackRawLink ? [fallbackRawLink] : []);

    const parsed = candidates
      .map((item) => this.parseSingleMessageLink(item))
      .filter((item): item is NonNullable<ReturnType<CloneChannelsService['parseSingleMessageLink']>> => Boolean(item));

    const dedupedMap = new Map<string, (typeof parsed)[number]>();
    for (const row of parsed) {
      const key = `${row.channelUsername}:${row.messageId.toString()}`;
      if (!dedupedMap.has(key)) dedupedMap.set(key, row);
    }

    return Array.from(dedupedMap.values());
  }

  private encodeSingleMessageLinks(links: string[]) {
    return JSON.stringify({ v: 1, links });
  }

  private decodeSingleMessageLinks(raw?: string | null) {
    const value = String(raw ?? '').trim();
    if (!value) return [] as string[];

    if (value.startsWith('{')) {
      try {
        const parsed = JSON.parse(value) as { v?: number; links?: string[] };
        if (Array.isArray(parsed.links)) {
          return parsed.links.map((item) => String(item || '').trim()).filter(Boolean);
        }
      } catch {
        // fallback to single link parser below
      }
    }

    return [value];
  }

  private assertTaskId(id: string) {
    if (!/^\d+$/.test(id)) {
      throw new BadRequestException('invalid task id');
    }

    return BigInt(id);
  }

  private parsePositiveInt(value: string | number | undefined, fallback: number, max?: number) {
    const parsed = Number.parseInt(String(value ?? ''), 10);
    if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
    if (max && parsed > max) return max;
    return parsed;
  }

  private parseDateOrUndefined(value?: string) {
    if (!value) return undefined;
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? undefined : parsed;
  }

  private computeInitialNextRunAt(scheduleType: 'once' | 'interval' | 'hourly' | 'daily') {
    if (scheduleType === 'once') return new Date();
    if (scheduleType === 'interval') return new Date();
    if (scheduleType === 'hourly') return new Date();
    if (scheduleType === 'daily') return new Date();
    return new Date();
  }

  private normalizeScheduleConfig(params: {
    scheduleType?: 'once' | 'interval' | 'hourly' | 'daily';
    intervalSeconds?: number | null;
  }) {
    const scheduleType = params.scheduleType ?? 'once';

    if (scheduleType === 'interval') {
      const intervalSeconds = params.intervalSeconds;
      if (!Number.isInteger(intervalSeconds) || intervalSeconds < 60 || intervalSeconds > 86400) {
        throw new BadRequestException('intervalSeconds must be an integer between 60 and 86400 when scheduleType=interval');
      }

      return {
        scheduleType,
        intervalSeconds,
      };
    }

    return {
      scheduleType,
      intervalSeconds: null,
    };
  }

  async createTask(dto: CreateCloneTaskDto, userId: string) {
    await this.assertHasActiveCrawlAccount();

    const channels = this.normalizeChannels(dto.channels);
    if (channels.length === 0) {
      throw new BadRequestException('channels must contain at least one valid item');
    }

    const singleMessageEnabled = dto.singleMessageEnabled ?? false;
    const parsedSingleMessages = singleMessageEnabled
      ? this.parseSingleMessageLinks(dto.singleMessageLinks, dto.singleMessageLink)
      : [];

    if (singleMessageEnabled && parsedSingleMessages.length === 0) {
      throw new BadRequestException('singleMessageLinks is required when singleMessageEnabled=true');
    }

    if (singleMessageEnabled && channels.length !== 1) {
      throw new BadRequestException('single-message clone task must contain exactly one channel');
    }

    if (singleMessageEnabled && parsedSingleMessages.some((item) => item.channelUsername !== channels[0])) {
      throw new BadRequestException('all singleMessageLinks must match the selected channel');
    }

    const normalizedSchedule = this.normalizeScheduleConfig({
      scheduleType: dto.scheduleType,
      intervalSeconds: dto.intervalSeconds,
    });

    const created = await this.taskModel.create({
      data: {
        name: dto.name,
        scheduleType: normalizedSchedule.scheduleType,
        scheduleCron: dto.scheduleCron,
        intervalSeconds: normalizedSchedule.intervalSeconds,
        timezone: dto.timezone ?? 'Asia/Shanghai',
        dailyRunTime: dto.dailyRunTime,
        nextRunAt: this.computeInitialNextRunAt(normalizedSchedule.scheduleType),
        crawlMode: dto.crawlMode ?? 'index_only',
        contentTypes: dto.contentTypes ?? ['text', 'image', 'video'],
        recentLimit: singleMessageEnabled ? 1 : dto.recentLimit ?? 100,
        singleMessageEnabled,
        singleMessageLink: singleMessageEnabled
          ? this.encodeSingleMessageLinks(parsedSingleMessages.map((item) => item.normalizedLink))
          : null,
        downloadMaxFileMb: dto.downloadMaxFileMb,
        globalDownloadConcurrency: dto.globalDownloadConcurrency ?? 4,
        retryMax: dto.retryMax ?? 5,
        targetPathType: dto.targetPathType,
        targetPath: dto.targetPath,
        createdBy: BigInt(userId),
        channels: {
          createMany: {
            data: channels.map((channelUsername) => ({
              channelUsername,
              channelAccessStatus: 'ok',
            })),
          },
        },
      },
      include: {
        channels: true,
      },
    });

    return this.serializeBigInt(created);
  }

  async listTasks(filters?: {
    keyword?: string;
    status?: string;
    crawlMode?: string;
    scheduleType?: string;
    createdBy?: string;
    updatedFrom?: string;
    updatedTo?: string;
  }) {
    const where: Record<string, any> = {};
    const keyword = String(filters?.keyword ?? '').trim();
    const status = String(filters?.status ?? '').trim();
    const crawlMode = String(filters?.crawlMode ?? '').trim();
    const scheduleType = String(filters?.scheduleType ?? '').trim();
    const createdByKeyword = String(filters?.createdBy ?? '').trim();
    const updatedFrom = this.parseDateOrUndefined(filters?.updatedFrom);
    const updatedTo = this.parseDateOrUndefined(filters?.updatedTo);

    if (keyword) {
      where.OR = [
        { name: { contains: keyword, mode: 'insensitive' } },
        {
          channels: {
            some: {
              channelUsername: { contains: keyword.replace(/^@+/, ''), mode: 'insensitive' },
            },
          },
        },
      ];
    }

    if (status) where.status = status;
    if (crawlMode) where.crawlMode = crawlMode;
    if (scheduleType) where.scheduleType = scheduleType;

    if (updatedFrom || updatedTo) {
      where.updatedAt = {
        ...(updatedFrom ? { gte: updatedFrom } : {}),
        ...(updatedTo ? { lte: updatedTo } : {}),
      };
    }

    if (createdByKeyword) {
      const users = await this.prisma.user.findMany({
        where: {
          username: {
            contains: createdByKeyword,
            mode: 'insensitive',
          },
        },
        select: { id: true },
        take: 20,
      });

      if (users.length === 0) {
        return [];
      }

      where.createdBy = { in: users.map((user) => user.id) };
    }

    const rows = await this.taskModel.findMany({
      where,
      include: {
        _count: {
          select: { channels: true, runs: true },
        },
        runs: {
          orderBy: { createdAt: 'desc' },
          take: 1,
          select: {
            id: true,
            status: true,
            channelSuccess: true,
            channelFailed: true,
            indexedCount: true,
            downloadedCount: true,
            dedupCount: true,
            createdAt: true,
          },
        },
      },
      orderBy: { updatedAt: 'desc' },
      take: 200,
    });

    const creatorIds = Array.from(
      new Set(
        rows
          .map((row) => row.createdBy)
          .filter((value): value is bigint => typeof value === 'bigint'),
      ),
    );

    const creators = creatorIds.length
      ? await this.prisma.user.findMany({
          where: { id: { in: creatorIds } },
          select: { id: true, username: true, displayName: true },
        })
      : [];

    const creatorMap = new Map(
      creators.map((user) => [
        user.id.toString(),
        user.displayName?.trim() || user.username,
      ]),
    );

    const enriched = rows.map((row) => ({
      ...row,
      createdByUsername: row.createdBy ? creatorMap.get(row.createdBy.toString()) ?? null : null,
      latestRun: row.runs[0] ?? null,
    }));

    return this.serializeBigInt(enriched);
  }

  async getTask(id: string) {
    const taskId = this.assertTaskId(id);
    const row = await this.taskModel.findUnique({
      where: { id: taskId },
      include: {
        channels: true,
        runs: {
          orderBy: { createdAt: 'desc' },
          take: 20,
        },
      },
    });

    if (!row) {
      throw new NotFoundException('clone task not found');
    }

    return this.serializeBigInt(row);
  }

  async updateTask(id: string, dto: UpdateCloneTaskDto) {
    const taskId = this.assertTaskId(id);
    const existingTask = await this.taskModel.findUnique({
      where: { id: taskId },
      include: {
        channels: {
          select: { channelUsername: true },
        },
      },
    });

    if (!existingTask) {
      throw new NotFoundException('clone task not found');
    }

    const normalizedChannels = dto.channels ? this.normalizeChannels(dto.channels) : null;
    if (normalizedChannels && normalizedChannels.length === 0) {
      throw new BadRequestException('channels must contain at least one valid item');
    }

    const mergedChannels =
      normalizedChannels ?? existingTask.channels.map((channel) => channel.channelUsername);
    const singleMessageEnabled = dto.singleMessageEnabled ?? existingTask.singleMessageEnabled;
    const existingLinks = this.decodeSingleMessageLinks(existingTask.singleMessageLink);
    const parsedSingleMessages = singleMessageEnabled
      ? this.parseSingleMessageLinks(
          dto.singleMessageLinks !== undefined ? dto.singleMessageLinks : undefined,
          dto.singleMessageLink !== undefined
            ? dto.singleMessageLink
            : (existingLinks[0] ?? null),
        )
      : [];

    if (singleMessageEnabled && dto.singleMessageLinks === undefined && dto.singleMessageLink === undefined && existingLinks.length > 1) {
      const fromExisting = this.parseSingleMessageLinks(existingLinks, null);
      if (fromExisting.length > 0) {
        parsedSingleMessages.splice(0, parsedSingleMessages.length, ...fromExisting);
      }
    }

    if (singleMessageEnabled && parsedSingleMessages.length === 0) {
      throw new BadRequestException('singleMessageLinks is required when singleMessageEnabled=true');
    }

    if (singleMessageEnabled && mergedChannels.length !== 1) {
      throw new BadRequestException('single-message clone task must contain exactly one channel');
    }

    if (singleMessageEnabled && parsedSingleMessages.some((item) => item.channelUsername !== mergedChannels[0])) {
      throw new BadRequestException('all singleMessageLinks must match the selected channel');
    }

    const nextScheduleType = (dto.scheduleType ?? existingTask.scheduleType) as 'once' | 'interval' | 'hourly' | 'daily';
    const normalizedSchedule = this.normalizeScheduleConfig({
      scheduleType: nextScheduleType,
      intervalSeconds: dto.intervalSeconds !== undefined ? dto.intervalSeconds : existingTask.intervalSeconds,
    });

    const updated = await this.prisma.$transaction(async (tx) => {
      const row = await tx.cloneCrawlTask.update({
        where: { id: taskId },
        data: {
          name: dto.name,
          status: dto.status,
          scheduleType: dto.scheduleType,
          scheduleCron: dto.scheduleCron,
          intervalSeconds: normalizedSchedule.intervalSeconds,
          timezone: dto.timezone,
          dailyRunTime: dto.dailyRunTime,
          nextRunAt: dto.status === 'running' ? new Date() : undefined,
          recentLimit: singleMessageEnabled ? 1 : dto.recentLimit,
          singleMessageEnabled,
          singleMessageLink: singleMessageEnabled
            ? this.encodeSingleMessageLinks(parsedSingleMessages.map((item) => item.normalizedLink))
            : null,
          crawlMode: dto.crawlMode,
          contentTypes: dto.contentTypes,
          downloadMaxFileMb: dto.downloadMaxFileMb,
          globalDownloadConcurrency: dto.globalDownloadConcurrency,
          retryMax: dto.retryMax,
          targetPathType: dto.targetPathType,
          targetPath: dto.targetPath,
        },
      });

      if (normalizedChannels) {
        await tx.cloneCrawlTaskChannel.deleteMany({ where: { taskId } });
        await tx.cloneCrawlTaskChannel.createMany({
          data: normalizedChannels.map((channelUsername) => ({
            taskId,
            channelUsername,
            channelAccessStatus: 'ok',
          })),
        });
      }

      return row;
    });

    return this.serializeBigInt(updated);
  }

  async pauseTask(id: string) {
    const taskId = this.assertTaskId(id);
    await this.getTask(id);
    const updated = await this.taskModel.update({
      where: { id: taskId },
      data: { status: 'paused', nextRunAt: null },
      select: { id: true, status: true, updatedAt: true },
    });

    return this.serializeBigInt(updated);
  }

  async resumeTask(id: string) {
    const taskId = this.assertTaskId(id);
    await this.getTask(id);
    const updated = await this.taskModel.update({
      where: { id: taskId },
      data: { status: 'running', nextRunAt: new Date() },
      select: { id: true, status: true, updatedAt: true },
    });

    return this.serializeBigInt(updated);
  }

  async runNow(id: string) {
    await this.assertHasActiveCrawlAccount();

    const taskId = this.assertTaskId(id);
    await this.getTask(id);

    await this.taskModel.update({
      where: { id: taskId },
      data: { status: 'running', nextRunAt: new Date() },
    });

    return this.serializeBigInt({
      taskId,
      status: 'running',
      queued: true,
    });
  }

  async deleteTask(id: string) {
    const taskId = this.assertTaskId(id);
    await this.getTask(id);

    await this.taskModel.delete({
      where: { id: taskId },
    });

    return { ok: true, id };
  }

  async validateChannels(channels: string[]) {
    const normalized = this.normalizeChannels(channels);
    const seen = new Set<string>();

    const valid: Array<{ channel: string; status: 'ok' }> = [];
    const invalid: Array<{ channel: string; status: string; reason: string }> = [];

    for (const item of normalized) {
      const username = item.replace(/^@/, '');
      if (!/^[a-zA-Z0-9_]{5,}$/.test(username)) {
        invalid.push({ channel: item, status: 'invalid', reason: '格式错误' });
        continue;
      }
      if (seen.has(item)) {
        invalid.push({ channel: item, status: 'duplicate', reason: '重复频道' });
        continue;
      }

      seen.add(item);
      valid.push({ channel: item, status: 'ok' });
    }

    return {
      total: channels.length,
      normalizedTotal: normalized.length,
      valid,
      invalid,
    };
  }

  async estimate(body: { channels?: string[]; recentLimit?: number; crawlMode?: string }) {
    const channels = this.normalizeChannels(body.channels ?? []);
    const recentLimit = body.recentLimit ?? 100;
    const estimatedIndexedCount = channels.length * recentLimit;
    const estimatedDownloadCount =
      body.crawlMode === 'index_and_download'
        ? Math.floor(estimatedIndexedCount * 0.12)
        : 0;

    return {
      channels: channels.length,
      recentLimit,
      estimatedIndexedCount,
      estimatedDownloadCount,
      model: 'basic_mvp_estimation',
    };
  }

  async listDownloadQueue(params?: {
    page?: string;
    pageSize?: string;
    failedOnly?: string;
  }) {
    const page = this.parsePositiveInt(params?.page, 1);
    const pageSize = this.parsePositiveInt(params?.pageSize, 20, 100);
    const failedOnly = String(params?.failedOnly ?? '').trim() === 'true';
    const statuses: CloneDownloadStatus[] = failedOnly
      ? ['failed_retryable', 'failed_final', 'paused_by_guard']
      : ['queued', 'downloading', 'downloaded', 'failed_retryable', 'failed_final', 'paused_by_guard'];
    const where = {
      downloadStatus: {
        in: statuses,
      },
    };

    const [rows, total, guardRows] = await Promise.all([
      this.prisma.cloneCrawlItem.findMany({
        where,
        orderBy: { updatedAt: 'desc' },
        skip: (page - 1) * pageSize,
        take: pageSize,
      }),
      this.prisma.cloneCrawlItem.count({ where }),
      this.prisma.cloneCrawlItem.findMany({
        where: {
          ...where,
          downloadStatus: 'paused_by_guard',
        },
        select: { downloadErrorCode: true },
      }),
    ]);

    const reasonCounter = new Map<string, number>();

    for (const row of guardRows) {
      const key = (row.downloadErrorCode || 'unknown').trim() || 'unknown';
      reasonCounter.set(key, (reasonCounter.get(key) || 0) + 1);
    }

    const guardTotal = guardRows.length;
    const reasonStats = Array.from(reasonCounter.entries())
      .map(([reasonCode, count]) => ({
        reasonCode,
        count,
        ratio: guardTotal > 0 ? Number(((count / guardTotal) * 100).toFixed(2)) : 0,
      }))
      .sort((a, b) => b.count - a.count);

    return this.serializeBigInt({
      items: rows,
      reasonStats,
      guardTotal,
      total,
      page,
      pageSize,
      totalPages: Math.max(1, Math.ceil(total / pageSize)),
    });
  }

  async listTaskFailures(id: string) {
    const taskId = this.assertTaskId(id);
    await this.getTask(id);

    const channelFailures = await this.prisma.cloneCrawlTaskChannel.findMany({
      where: {
        taskId,
        OR: [
          { channelAccessStatus: { in: ['private', 'not_found', 'invalid'] } },
          { lastErrorMessage: { not: null } },
        ],
      },
      orderBy: { updatedAt: 'desc' },
      take: 200,
      select: {
        id: true,
        channelUsername: true,
        channelAccessStatus: true,
        lastErrorCode: true,
        lastErrorMessage: true,
        updatedAt: true,
      },
    });

    const downloadFailures = await this.prisma.cloneCrawlItem.findMany({
      where: {
        taskId,
        downloadStatus: { in: ['failed_retryable', 'failed_final', 'paused_by_guard'] },
      },
      orderBy: { updatedAt: 'desc' },
      take: 200,
      select: {
        id: true,
        channelUsername: true,
        messageId: true,
        downloadStatus: true,
        downloadErrorCode: true,
        downloadError: true,
        updatedAt: true,
      },
    });

    return this.serializeBigInt({ channelFailures, downloadFailures });
  }

  async listTaskPreview(id: string) {
    const taskId = this.assertTaskId(id);
    await this.getTask(id);

    const rows = await this.prisma.cloneCrawlItem.findMany({
      where: { taskId },
      orderBy: { createdAt: 'desc' },
      take: 100,
      select: {
        id: true,
        channelUsername: true,
        messageId: true,
        hasVideo: true,
        mimeType: true,
        fileSize: true,
        downloadStatus: true,
        messageDate: true,
        createdAt: true,
      },
    });

    return this.serializeBigInt(rows);
  }

  async listTaskLogs(id: string) {
    const taskId = this.assertTaskId(id);
    await this.getTask(id);

    const runs = await this.prisma.cloneCrawlRun.findMany({
      where: { taskId },
      orderBy: { createdAt: 'desc' },
      take: 30,
      select: {
        id: true,
        status: true,
        channelTotal: true,
        channelSuccess: true,
        channelFailed: true,
        indexedCount: true,
        downloadedCount: true,
        createdAt: true,
        startedAt: true,
        finishedAt: true,
      },
    });

    const items = await this.prisma.cloneCrawlItem.findMany({
      where: { taskId },
      orderBy: { updatedAt: 'desc' },
      take: 50,
      select: {
        id: true,
        channelUsername: true,
        messageId: true,
        downloadStatus: true,
        downloadErrorCode: true,
        updatedAt: true,
      },
    });

    const logs = [
      ...runs.map((run) => ({
        at: run.finishedAt ?? run.startedAt ?? run.createdAt,
        level: run.status === 'failed' ? 'error' : 'info',
        type: 'run',
        message: `Run#${run.id} status=${run.status}, channels=${run.channelSuccess}/${run.channelTotal}, indexed=${run.indexedCount}, downloaded=${run.downloadedCount}`,
      })),
      ...items.map((item) => ({
        at: item.updatedAt,
        level:
          item.downloadStatus === 'failed_final' || item.downloadStatus === 'failed_retryable'
            ? 'warn'
            : 'info',
        type: 'item',
        message: `Item#${item.id} @${item.channelUsername.replace(/^@/, '')} msg=${item.messageId} download=${item.downloadStatus}${item.downloadErrorCode ? ` (${item.downloadErrorCode})` : ''}`,
      })),
    ].sort((a, b) => new Date(b.at).getTime() - new Date(a.at).getTime());

    return this.serializeBigInt(logs.slice(0, 120));
  }

  async retryDownloadQueue(ids: string[]) {
    const parsedIds = ids.filter((id) => /^\d+$/.test(id)).map((id) => BigInt(id));
    if (parsedIds.length === 0) {
      throw new BadRequestException('ids is required');
    }

    const updated = await this.prisma.cloneCrawlItem.updateMany({
      where: { id: { in: parsedIds } },
      data: {
        downloadStatus: 'queued',
        downloadErrorCode: null,
        downloadError: null,
      },
    });

    return { retried: updated.count };
  }

  async deleteDownloadQueue(ids: string[]) {
    const parsedIds = ids.filter((id) => /^\d+$/.test(id)).map((id) => BigInt(id));
    if (parsedIds.length === 0) {
      throw new BadRequestException('ids is required');
    }

    const itemRows = await this.prisma.cloneCrawlItem.findMany({
      where: { id: { in: parsedIds } },
      select: {
        id: true,
        downloadStatus: true,
      },
    });

    const itemIdSet = new Set(itemRows.map((row) => row.id.toString()));
    const existingIds = parsedIds.filter((id) => itemIdSet.has(id.toString()));

    if (existingIds.length === 0) {
      return {
        deleted: 0,
        cancelledRunning: 0,
        removedQueueJobs: 0,
        removedGuardWaitJobs: 0,
        removedRetryJobs: 0,
      };
    }

    const runningStatuses: CloneDownloadStatus[] = ['downloading', 'queued', 'failed_retryable', 'paused_by_guard'];

    const cancelled = await this.prisma.cloneCrawlItem.updateMany({
      where: {
        id: { in: existingIds },
        downloadStatus: {
          in: runningStatuses,
        },
      },
      data: {
        downloadStatus: 'failed_final',
        downloadLeaseUntil: null,
        downloadHeartbeatAt: null,
        downloadWorkerJobId: null,
        downloadErrorCode: 'manual_deleted',
        downloadError: 'Deleted by operator (force delete)',
      } as any,
    });

    const itemIdStrings = existingIds.map((id) => id.toString());

    const removeMainQueue = itemIdStrings.map(async (itemId) => {
      const job = await this.cloneMediaDownloadQueue.getJob(`clone-download-item-${itemId}`);
      if (!job) return 0;
      try {
        await job.remove();
        return 1;
      } catch {
        return 0;
      }
    });

    const removeGuardQueue = itemIdStrings.map(async (itemId) => {
      const job = await this.cloneGuardWaitQueue.getJob(`clone-download-item-${itemId}`);
      if (!job) return 0;
      try {
        await job.remove();
        return 1;
      } catch {
        return 0;
      }
    });

    const retryJobs = await this.cloneRetryQueue.getJobs(['waiting', 'delayed', 'prioritized', 'waiting-children']);
    let removedRetryJobs = 0;

    const retryTargets = new Set(itemIdStrings);
    for (const job of retryJobs) {
      const data = job.data as {
        payload?: {
          itemId?: string;
        };
      };

      const retryItemId = String(data?.payload?.itemId ?? '').trim();
      if (!retryItemId || !retryTargets.has(retryItemId)) continue;

      try {
        await job.remove();
        removedRetryJobs += 1;
      } catch {
        // ignore remove race
      }
    }

    const [mainRemovedStats, guardRemovedStats, deleted] = await Promise.all([
      Promise.all(removeMainQueue),
      Promise.all(removeGuardQueue),
      this.prisma.cloneCrawlItem.deleteMany({
        where: { id: { in: existingIds } },
      }),
    ]);

    const removedQueueJobs = mainRemovedStats.reduce((sum, curr) => sum + curr, 0);
    const removedGuardWaitJobs = guardRemovedStats.reduce((sum, curr) => sum + curr, 0);

    return {
      deleted: deleted.count,
      cancelledRunning: cancelled.count,
      removedQueueJobs,
      removedGuardWaitJobs,
      removedRetryJobs,
    };
  }

  async pauseAllDownloads() {
    const updated = await this.prisma.cloneCrawlItem.updateMany({
      where: {
        downloadStatus: {
          in: ['queued', 'downloading'],
        },
      },
      data: {
        downloadStatus: 'paused_by_guard',
        downloadErrorCode: 'manual_pause_all',
        downloadError: 'Paused by operator',
      },
    });

    return { paused: updated.count };
  }

  async batchResumeTasks(ids: string[]) {
    const parsedIds = ids.filter((id) => /^\d+$/.test(id)).map((id) => BigInt(id));
    if (parsedIds.length === 0) {
      throw new BadRequestException('ids is required');
    }

    const updated = await this.taskModel.updateMany({
      where: { id: { in: parsedIds } },
      data: { status: 'running', nextRunAt: new Date() },
    });

    return { resumed: updated.count };
  }

  async batchPauseTasks(ids: string[]) {
    const parsedIds = ids.filter((id) => /^\d+$/.test(id)).map((id) => BigInt(id));
    if (parsedIds.length === 0) {
      throw new BadRequestException('ids is required');
    }

    const updated = await this.taskModel.updateMany({
      where: { id: { in: parsedIds } },
      data: { status: 'paused', nextRunAt: null },
    });

    return { paused: updated.count };
  }

  async batchRunNow(ids: string[]) {
    const parsedIds = ids.filter((id) => /^\d+$/.test(id)).map((id) => BigInt(id));
    if (parsedIds.length === 0) {
      throw new BadRequestException('ids is required');
    }

    const updated = await this.taskModel.updateMany({
      where: { id: { in: parsedIds } },
      data: { status: 'running', nextRunAt: new Date() },
    });

    return { queued: updated.count };
  }

  async batchRetryFailed(ids: string[]) {
    const parsedIds = ids.filter((id) => /^\d+$/.test(id)).map((id) => BigInt(id));
    if (parsedIds.length === 0) {
      throw new BadRequestException('ids is required');
    }

    const updated = await this.prisma.cloneCrawlItem.updateMany({
      where: {
        taskId: { in: parsedIds },
        downloadStatus: { in: ['failed_retryable', 'failed_final', 'paused_by_guard'] },
      },
      data: {
        downloadStatus: 'queued',
        downloadErrorCode: null,
        downloadError: null,
      },
    });

    return { retried: updated.count };
  }

  async resumeAllDownloads() {
    const updated = await this.prisma.cloneCrawlItem.updateMany({
      where: { downloadStatus: 'paused_by_guard' },
      data: {
        downloadStatus: 'queued',
        downloadErrorCode: null,
        downloadError: null,
      },
    });

    return { resumed: updated.count };
  }

  async sendAccountCode(body: { phone?: string }) {
    const phone = String(body.phone ?? '').trim();
    if (!/^\+?\d{6,20}$/.test(phone)) {
      throw new BadRequestException('invalid phone');
    }

    const { sentPhoneCodeHash, tempSession } = await this.withTempTelegramClient(async (client) => {
      const r = await client.invoke(
        new Api.auth.SendCode({
          phoneNumber: phone,
          apiId: this.getGramjsBaseConfig().apiId,
          apiHash: this.getGramjsBaseConfig().apiHash,
          settings: new Api.CodeSettings({
            allowAppHash: true,
          }),
        }),
      );

      if (!('phoneCodeHash' in r) || !r.phoneCodeHash) {
        throw new BadRequestException('发送验证码失败：未返回 phoneCodeHash');
      }

      return {
        sentPhoneCodeHash: String(r.phoneCodeHash),
        tempSession: (client.session as StringSession).save(),
      };
    });

    this.loginFlowStore.set(`${phone}:${sentPhoneCodeHash}`, {
      phoneCodeHashFromTg: sentPhoneCodeHash,
      tempSession,
      createdAt: Date.now(),
    });

    this.logger.log(`clone-account send-code flow saved: phone=${phone}, tgHash=${sentPhoneCodeHash.slice(0, 6)}***`);

    return {
      phone,
      phoneCodeHash: sentPhoneCodeHash,
      message: '验证码已发送，请输入验证码完成登录',
    };
  }

  async accountSignIn(body: { phone?: string; phoneCodeHash?: string; code?: string; password?: string }) {
    const phone = String(body.phone ?? '').trim();
    const phoneCodeHash = String(body.phoneCodeHash ?? '').trim();
    const code = String(body.code ?? '').trim();

    if (!/^\+?\d{6,20}$/.test(phone)) {
      throw new BadRequestException('invalid phone');
    }
    if (!phoneCodeHash || !code) {
      throw new BadRequestException('phoneCodeHash and code are required');
    }

    const flow = this.loginFlowStore.get(`${phone}:${phoneCodeHash}`);
    if (!flow) {
      throw new BadRequestException('登录流程不存在或已过期，请重新发送验证码');
    }

    const flowAgeMs = Date.now() - flow.createdAt;
    this.logger.log(`clone-account sign-in attempt: phone=${phone}, hashPrefix=${phoneCodeHash.slice(0, 6)}***, flowAgeSec=${Math.floor(flowAgeMs / 1000)}`);

    if (flowAgeMs > 5 * 60_000) {
      this.loginFlowStore.delete(`${phone}:${phoneCodeHash}`);
      throw new BadRequestException('验证码流程已过期（服务端5分钟TTL），请重新发送验证码');
    }

    const sessionString = await this.withTempTelegramClient(async (client) => {
      try {
        const auth = await client.invoke(
          new Api.auth.SignIn({
            phoneNumber: phone,
            phoneCodeHash: flow.phoneCodeHashFromTg,
            phoneCode: code,
          }),
        );

        if (!auth) {
          throw new BadRequestException('登录失败：未获取到授权结果');
        }
      } catch (e) {
        const msg = (e instanceof Error ? e.message : String(e)).toLowerCase();

        if (msg.includes('session_password_needed')) {
          const password = String(body.password ?? '').trim();
          if (!password) {
            throw new BadRequestException('SESSION_PASSWORD_NEEDED: 该账号开启了二步验证，请提供 password');
          }

          const pwd = await client.invoke(new Api.account.GetPassword());
          const inputCheck = await client.invoke(
            new Api.auth.CheckPassword({
              password: await computeCheck(pwd, password),
            }),
          );

          if (!inputCheck) {
            throw new BadRequestException('二步验证失败');
          }
          return (client.session as StringSession).save();
        }

        if (msg.includes('phone_code_expired')) {
          throw new BadRequestException('PHONE_CODE_EXPIRED: 验证码已过期，请重新发送验证码');
        }

        if (msg.includes('phone_code_invalid')) {
          throw new BadRequestException('PHONE_CODE_INVALID: 验证码错误，请重新输入');
        }

        if (msg.includes('phone_number_unoccupied')) {
          throw new BadRequestException('PHONE_NUMBER_UNOCCUPIED: 该手机号未注册 Telegram 账号');
        }

        throw e;
      }

      return (client.session as StringSession).save();
    }, flow.tempSession);

    const upserted = await this.prisma.cloneCrawlAccount.upsert({
      where: { accountPhone: phone },
      create: {
        accountPhone: phone,
        accountType: 'user',
        sessionString: this.encryptSession(sessionString),
        status: 'active',
        lastLoginAt: new Date(),
        lastCheckAt: new Date(),
      },
      update: {
        accountType: 'user',
        sessionString: this.encryptSession(sessionString),
        status: 'active',
        lastLoginAt: new Date(),
        lastCheckAt: new Date(),
        lastErrorCode: null,
        lastErrorMessage: null,
      },
      select: {
        id: true,
        accountPhone: true,
        accountType: true,
        status: true,
        lastLoginAt: true,
        lastCheckAt: true,
      },
    });

    this.loginFlowStore.delete(`${phone}:${phoneCodeHash}`);
    return this.serializeBigInt(upserted);
  }

  async listAccounts() {
    const rows = await this.prisma.cloneCrawlAccount.findMany({
      orderBy: { updatedAt: 'desc' },
      take: 100,
      select: {
        id: true,
        accountPhone: true,
        accountType: true,
        status: true,
        lastLoginAt: true,
        lastCheckAt: true,
        lastErrorCode: true,
        lastErrorMessage: true,
        createdAt: true,
        updatedAt: true,
      },
    });

    return this.serializeBigInt(rows);
  }

  async verifyAccount(id: string) {
    const accountId = this.assertTaskId(id);

    const account = await this.prisma.cloneCrawlAccount.findUnique({
      where: { id: accountId },
      select: { id: true, accountPhone: true, status: true },
    });

    if (!account) {
      throw new NotFoundException('clone crawl account not found');
    }

    const updated = await this.prisma.cloneCrawlAccount.update({
      where: { id: accountId },
      data: {
        status: 'active',
        lastCheckAt: new Date(),
        lastErrorCode: null,
        lastErrorMessage: null,
      },
      select: {
        id: true,
        accountPhone: true,
        accountType: true,
        status: true,
        lastLoginAt: true,
        lastCheckAt: true,
      },
    });

    return this.serializeBigInt(updated);
  }

  async logoutAccount(id: string) {
    const accountId = this.assertTaskId(id);

    const account = await this.prisma.cloneCrawlAccount.findUnique({
      where: { id: accountId },
      select: { id: true, accountPhone: true },
    });

    if (!account) {
      throw new NotFoundException('clone crawl account not found');
    }

    const updated = await this.prisma.cloneCrawlAccount.update({
      where: { id: accountId },
      data: {
        status: 'expired',
        sessionString: '',
        lastCheckAt: new Date(),
        lastErrorCode: 'manual_logout',
        lastErrorMessage: 'Logged out by operator',
      },
      select: {
        id: true,
        accountPhone: true,
        accountType: true,
        status: true,
        lastLoginAt: true,
        lastCheckAt: true,
      },
    });

    return this.serializeBigInt(updated);
  }
}

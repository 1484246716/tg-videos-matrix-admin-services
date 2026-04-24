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
import { PrismaService } from '../prisma/prisma.service';
import { CreateCloneTaskDto } from './dto/create-clone-task.dto';
import { UpdateCloneTaskDto } from './dto/update-clone-task.dto';

@Injectable()
export class CloneChannelsService {
  private readonly logger = new Logger(CloneChannelsService.name);

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

  private assertTaskId(id: string) {
    if (!/^\d+$/.test(id)) {
      throw new BadRequestException('invalid task id');
    }

    return BigInt(id);
  }

  private computeInitialNextRunAt(scheduleType: 'once' | 'hourly' | 'daily') {
    if (scheduleType === 'once') return new Date();
    if (scheduleType === 'hourly') return new Date();
    if (scheduleType === 'daily') return new Date();
    return new Date();
  }

  async createTask(dto: CreateCloneTaskDto, userId: string) {
    await this.assertHasActiveCrawlAccount();

    const channels = this.normalizeChannels(dto.channels);
    if (channels.length === 0) {
      throw new BadRequestException('channels must contain at least one valid item');
    }

    const singleMessageEnabled = dto.singleMessageEnabled ?? false;
    const parsedSingleMessage = singleMessageEnabled
      ? this.parseSingleMessageLink(dto.singleMessageLink)
      : null;

    if (singleMessageEnabled && !parsedSingleMessage) {
      throw new BadRequestException('singleMessageLink is required when singleMessageEnabled=true');
    }

    if (singleMessageEnabled && channels.length !== 1) {
      throw new BadRequestException('single-message clone task must contain exactly one channel');
    }

    if (singleMessageEnabled && channels[0] !== parsedSingleMessage.channelUsername) {
      throw new BadRequestException('singleMessageLink must match the selected channel');
    }

    const scheduleType = dto.scheduleType ?? 'once';

    const created = await this.taskModel.create({
      data: {
        name: dto.name,
        scheduleType,
        scheduleCron: dto.scheduleCron,
        timezone: dto.timezone ?? 'Asia/Shanghai',
        dailyRunTime: dto.dailyRunTime,
        nextRunAt: this.computeInitialNextRunAt(scheduleType),
        crawlMode: dto.crawlMode ?? 'index_only',
        contentTypes: dto.contentTypes ?? ['text', 'image', 'video'],
        recentLimit: singleMessageEnabled ? 1 : dto.recentLimit ?? 100,
        singleMessageEnabled,
        singleMessageLink: singleMessageEnabled ? parsedSingleMessage.normalizedLink : null,
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

  async listTasks() {
    const rows = await this.taskModel.findMany({
      include: {
        _count: {
          select: { channels: true, runs: true },
        },
      },
      orderBy: { updatedAt: 'desc' },
      take: 100,
    });

    return this.serializeBigInt(rows);
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
    const parsedSingleMessage = singleMessageEnabled
      ? this.parseSingleMessageLink(
          dto.singleMessageLink !== undefined
            ? dto.singleMessageLink
            : existingTask.singleMessageLink,
        )
      : null;

    if (singleMessageEnabled && !parsedSingleMessage) {
      throw new BadRequestException('singleMessageLink is required when singleMessageEnabled=true');
    }

    if (singleMessageEnabled && mergedChannels.length !== 1) {
      throw new BadRequestException('single-message clone task must contain exactly one channel');
    }

    if (singleMessageEnabled && mergedChannels[0] !== parsedSingleMessage.channelUsername) {
      throw new BadRequestException('singleMessageLink must match the selected channel');
    }

    const updated = await this.prisma.$transaction(async (tx) => {
      const row = await tx.cloneCrawlTask.update({
        where: { id: taskId },
        data: {
          name: dto.name,
          status: dto.status,
          scheduleType: dto.scheduleType,
          scheduleCron: dto.scheduleCron,
          timezone: dto.timezone,
          dailyRunTime: dto.dailyRunTime,
          nextRunAt: dto.status === 'running' ? new Date() : undefined,
          recentLimit: singleMessageEnabled ? 1 : dto.recentLimit,
          singleMessageEnabled,
          singleMessageLink: singleMessageEnabled ? parsedSingleMessage.normalizedLink : null,
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

  async listDownloadQueue() {
    const rows = await this.prisma.cloneCrawlItem.findMany({
      where: {
        downloadStatus: {
          in: ['queued', 'downloading', 'downloaded', 'failed_retryable', 'failed_final', 'paused_by_guard'],
        },
      },
      orderBy: { updatedAt: 'desc' },
      take: 200,
    });

    const reasonCounter = new Map<string, number>();
    const guardRows = rows.filter((row) => row.downloadStatus === 'paused_by_guard');

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

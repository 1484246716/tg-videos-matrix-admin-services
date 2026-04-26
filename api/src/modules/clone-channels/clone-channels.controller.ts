import { Body, Controller, Delete, Get, Param, Patch, Post, Query, Request, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { CloneChannelsService } from './clone-channels.service';
import { CreateCloneTaskDto } from './dto/create-clone-task.dto';
import { UpdateCloneTaskDto } from './dto/update-clone-task.dto';

interface AuthRequest {
  user: { userId: string; username: string; role: string };
}

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('clone-channels')
export class CloneChannelsController {
  constructor(private readonly cloneChannelsService: CloneChannelsService) {}

  @Permissions('clone-channels:create')
  @Post('tasks')
  createTask(@Body() dto: CreateCloneTaskDto, @Request() req: AuthRequest) {
    return this.cloneChannelsService.createTask(dto, req.user.userId);
  }

  @Permissions('clone-channels:view')
  @Get('tasks')
  listTasks(
    @Query('keyword') keyword?: string,
    @Query('status') status?: string,
    @Query('crawlMode') crawlMode?: string,
    @Query('scheduleType') scheduleType?: string,
    @Query('createdBy') createdBy?: string,
    @Query('updatedFrom') updatedFrom?: string,
    @Query('updatedTo') updatedTo?: string,
  ) {
    return this.cloneChannelsService.listTasks({
      keyword,
      status,
      crawlMode,
      scheduleType,
      createdBy,
      updatedFrom,
      updatedTo,
    });
  }

  @Permissions('channels:view')
  @Get('tasks/:id')
  getTask(@Param('id') id: string) {
    return this.cloneChannelsService.getTask(id);
  }

  @Permissions('clone-channels:update')
  @Patch('tasks/:id')
  updateTask(@Param('id') id: string, @Body() dto: UpdateCloneTaskDto) {
    return this.cloneChannelsService.updateTask(id, dto);
  }

  @Permissions('channels:update')
  @Post('tasks/:id/pause')
  pauseTask(@Param('id') id: string) {
    return this.cloneChannelsService.pauseTask(id);
  }

  @Permissions('channels:update')
  @Post('tasks/:id/resume')
  resumeTask(@Param('id') id: string) {
    return this.cloneChannelsService.resumeTask(id);
  }

  @Permissions('channels:update')
  @Post('tasks/:id/run-now')
  runNow(@Param('id') id: string) {
    return this.cloneChannelsService.runNow(id);
  }

  @Permissions('channels:update')
  @Delete('tasks/:id')
  deleteTask(@Param('id') id: string) {
    return this.cloneChannelsService.deleteTask(id);
  }

  @Permissions('channels:view')
  @Post('validate-channels')
  validateChannels(@Body() body: { channels?: string[] }) {
    return this.cloneChannelsService.validateChannels(body.channels ?? []);
  }

  @Permissions('channels:view')
  @Post('tasks/estimate')
  estimate(@Body() body: { channels?: string[]; recentLimit?: number; crawlMode?: string }) {
    return this.cloneChannelsService.estimate(body);
  }

  @Permissions('channels:view')
  @Get('download-queue')
  listDownloadQueue(
    @Query('page') page?: string,
    @Query('pageSize') pageSize?: string,
    @Query('failedOnly') failedOnly?: string,
  ) {
    return this.cloneChannelsService.listDownloadQueue({
      page,
      pageSize,
      failedOnly,
    });
  }

  @Permissions('channels:view')
  @Get('tasks/:id/failures')
  listTaskFailures(@Param('id') id: string) {
    return this.cloneChannelsService.listTaskFailures(id);
  }

  @Permissions('channels:view')
  @Get('tasks/:id/preview')
  listTaskPreview(@Param('id') id: string) {
    return this.cloneChannelsService.listTaskPreview(id);
  }

  @Permissions('channels:view')
  @Get('tasks/:id/logs')
  listTaskLogs(@Param('id') id: string) {
    return this.cloneChannelsService.listTaskLogs(id);
  }

  @Permissions('channels:update')
  @Post('download-queue/retry')
  retryDownloadQueue(@Body() body: { ids?: string[] }) {
    return this.cloneChannelsService.retryDownloadQueue(body.ids ?? []);
  }

  @Permissions('channels:update')
  @Post('download-queue/delete')
  deleteDownloadQueue(@Body() body: { ids?: string[] }) {
    return this.cloneChannelsService.deleteDownloadQueue(body.ids ?? []);
  }

  @Permissions('channels:update')
  @Post('tasks/batch-resume')
  batchResumeTasks(@Body() body: { ids?: string[] }) {
    return this.cloneChannelsService.batchResumeTasks(body.ids ?? []);
  }

  @Permissions('channels:update')
  @Post('tasks/batch-pause')
  batchPauseTasks(@Body() body: { ids?: string[] }) {
    return this.cloneChannelsService.batchPauseTasks(body.ids ?? []);
  }

  @Permissions('channels:update')
  @Post('tasks/batch-run-now')
  batchRunNow(@Body() body: { ids?: string[] }) {
    return this.cloneChannelsService.batchRunNow(body.ids ?? []);
  }

  @Permissions('channels:update')
  @Post('tasks/batch-retry-failed')
  batchRetryFailed(@Body() body: { ids?: string[] }) {
    return this.cloneChannelsService.batchRetryFailed(body.ids ?? []);
  }

  @Permissions('channels:update')
  @Post('download-queue/pause-all')
  pauseAllDownloads() {
    return this.cloneChannelsService.pauseAllDownloads();
  }

  @Permissions('channels:update')
  @Post('download-queue/resume-all')
  resumeAllDownloads() {
    return this.cloneChannelsService.resumeAllDownloads();
  }

  @Permissions('channels:update')
  @Post('accounts/send-code')
  sendAccountCode(@Body() body: { phone?: string }) {
    return this.cloneChannelsService.sendAccountCode(body);
  }

  @Permissions('channels:update')
  @Post('accounts/sign-in')
  accountSignIn(@Body() body: { phone?: string; phoneCodeHash?: string; code?: string; password?: string }) {
    return this.cloneChannelsService.accountSignIn(body);
  }

  @Permissions('channels:view')
  @Get('accounts')
  listAccounts() {
    return this.cloneChannelsService.listAccounts();
  }

  @Permissions('channels:update')
  @Post('accounts/:id/verify')
  verifyAccount(@Param('id') id: string) {
    return this.cloneChannelsService.verifyAccount(id);
  }

  @Permissions('channels:update')
  @Post('accounts/:id/logout')
  logoutAccount(@Param('id') id: string) {
    return this.cloneChannelsService.logoutAccount(id);
  }
}

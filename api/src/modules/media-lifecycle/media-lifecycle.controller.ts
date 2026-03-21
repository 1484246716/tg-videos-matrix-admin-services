import { Body, Controller, Delete, Get, Param, Query, Post, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { MediaLifecycleService } from './media-lifecycle.service';

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('media-lifecycle')
export class MediaLifecycleController {
  constructor(private readonly mediaLifecycleService: MediaLifecycleService) {}

  @Permissions('media-lifecycle:view')
  @Get()
  list(
    @Query('channelId') channelId?: string,
    @Query('telegramFileId') telegramFileId?: string,
    @Query('keyword') keyword?: string,
    @Query('stage') stage?: string,
    @Query('limit') limit?: string,
  ) {
    return this.mediaLifecycleService.list({
      channelId,
      telegramFileId,
      keyword,
      stage,
      limit: limit ? Number(limit) : undefined,
    });
  }

  @Permissions('media-lifecycle:view')
  @Get('progress')
  getProgress(@Query('ids') ids?: string) {
    const list = ids ? ids.split(',').map((id) => id.trim()).filter(Boolean) : [];
    return this.mediaLifecycleService.getProgress(list);
  }

  @Permissions('media-lifecycle:view')
  @Get(':id')
  getDetail(@Param('id') id: string) {
    return this.mediaLifecycleService.getDetail(id);
  }

  @Permissions('media-lifecycle:update')
  @Post(':id/retry-relay')
  retryRelay(@Param('id') id: string) {
    return this.mediaLifecycleService.retryRelay(id);
  }

  @Permissions('media-lifecycle:update')
  @Post('retry-relay')
  retryRelayBatch(@Body('ids') ids: string[]) {
    return this.mediaLifecycleService.retryRelayBatch(ids ?? []);
  }

  @Permissions('media-lifecycle:delete')
  @Delete(':id')
  remove(@Param('id') id: string, @Query('force') force?: string) {
    return this.mediaLifecycleService.remove(id, force === '1' || force === 'true');
  }
}

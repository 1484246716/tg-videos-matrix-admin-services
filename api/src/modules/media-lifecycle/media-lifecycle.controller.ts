import { Body, Controller, Get, Param, Query, Post, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { MediaLifecycleService } from './media-lifecycle.service';

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('media-lifecycle')
export class MediaLifecycleController {
  constructor(private readonly mediaLifecycleService: MediaLifecycleService) {}

  @Permissions('videos:view')
  @Get()
  list(
    @Query('channelId') channelId?: string,
    @Query('keyword') keyword?: string,
    @Query('stage') stage?: string,
    @Query('limit') limit?: string,
  ) {
    return this.mediaLifecycleService.list({
      channelId,
      keyword,
      stage,
      limit: limit ? Number(limit) : undefined,
    });
  }

  @Permissions('videos:view')
  @Get(':id')
  getDetail(@Param('id') id: string) {
    return this.mediaLifecycleService.getDetail(id);
  }

  @Permissions('videos:update')
  @Post(':id/retry-relay')
  retryRelay(@Param('id') id: string) {
    return this.mediaLifecycleService.retryRelay(id);
  }

  @Permissions('videos:update')
  @Post('retry-relay')
  retryRelayBatch(@Body('ids') ids: string[]) {
    return this.mediaLifecycleService.retryRelayBatch(ids ?? []);
  }
}

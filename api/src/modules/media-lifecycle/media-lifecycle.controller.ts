import { Body, Controller, Delete, Get, Param, Query, Post, Request, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { MediaLifecycleService } from './media-lifecycle.service';

interface AuthRequest {
  user: { userId: string; username: string; role: string };
}

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
    @Query('mediaType') mediaType?: string,
    @Query('limit') limit?: string,
    @Request() req?: AuthRequest,
  ) {
    return this.mediaLifecycleService.list({
      channelId,
      telegramFileId,
      keyword,
      stage,
      mediaType,
      limit: limit ? Number(limit) : undefined,
      userId: req?.user.userId,
      role: req?.user.role,
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
  getDetail(@Param('id') id: string, @Request() req: AuthRequest) {
    return this.mediaLifecycleService.getDetail(id, req.user.userId, req.user.role);
  }

  @Permissions('media-lifecycle:update')
  @Post(':id/retry-relay')
  retryRelay(@Param('id') id: string, @Request() req: AuthRequest) {
    return this.mediaLifecycleService.retryRelay(id, req.user.userId, req.user.role);
  }

  @Permissions('media-lifecycle:update')
  @Post('retry-relay')
  retryRelayBatch(@Body('ids') ids: string[], @Request() req: AuthRequest) {
    return this.mediaLifecycleService.retryRelayBatch(ids ?? [], req.user.userId, req.user.role);
  }

  @Permissions('media-lifecycle:delete')
  @Delete(':id')
  remove(@Param('id') id: string, @Query('force') force?: string, @Request() req?: AuthRequest) {
    return this.mediaLifecycleService.remove(id, force === '1' || force === 'true', req?.user.userId, req?.user.role);
  }
}

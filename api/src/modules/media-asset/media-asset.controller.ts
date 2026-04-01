import { Body, Controller, Get, Param, Patch, Post, Query, Request, UseGuards } from '@nestjs/common';
import { MediaStatus } from '@prisma/client';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { MediaAssetService } from './media-asset.service';
import { CreateMediaAssetDto } from './dto/create-media-asset.dto';
import { UpdateMediaAssetStatusDto } from './dto/update-media-asset-status.dto';
import { MarkRelayUploadedDto } from './dto/mark-relay-uploaded.dto';
import { BatchEnqueueRelayUploadDto } from './dto/batch-enqueue-relay-upload.dto';

interface AuthRequest {
  user: { userId: string; username: string; role: string };
}

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('media-assets')
export class MediaAssetController {
  constructor(private readonly mediaAssetService: MediaAssetService) {}

  @Permissions('media:view')
  @Get()
  list(
    @Query('channelId') channelId?: string,
    @Query('status') status?: MediaStatus,
    @Query('keyword') keyword?: string,
    @Query('limit') limit?: string,
    @Query('page') page?: string,
    @Query('pageSize') pageSize?: string,
    @Request() req?: AuthRequest,
  ) {
    return this.mediaAssetService.list({
      channelId,
      status,
      keyword,
      limit: limit ? Number(limit) : undefined,
      page: page ? Number(page) : undefined,
      pageSize: pageSize ? Number(pageSize) : undefined,
      userId: req?.user.userId,
      role: req?.user.role,
    });
  }

  @Permissions('media:upload')
  @Post()
  create(@Body() dto: CreateMediaAssetDto) {
    return this.mediaAssetService.create(dto);
  }

  @Permissions('media:update')
  @Patch(':id/status')
  updateStatus(@Param('id') id: string, @Body() dto: UpdateMediaAssetStatusDto) {
    return this.mediaAssetService.updateStatus(id, dto);
  }

  @Permissions('media:update')
  @Patch(':id/relay-uploaded')
  markRelayUploaded(@Param('id') id: string, @Body() dto: MarkRelayUploadedDto) {
    return this.mediaAssetService.markRelayUploaded(id, dto);
  }

  @Permissions('media:upload')
  @Post('relay-upload/batch-enqueue')
  batchEnqueueRelayUpload(@Body() dto: BatchEnqueueRelayUploadDto) {
    return this.mediaAssetService.batchEnqueueRelayUpload(dto);
  }
}

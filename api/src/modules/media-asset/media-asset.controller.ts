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

  @Permissions('videos:view')
  @Get()
  list(
    @Query('channelId') channelId?: string,
    @Query('status') status?: MediaStatus,
    @Query('limit') limit?: string,
    @Request() req?: AuthRequest,
  ) {
    return this.mediaAssetService.list({
      channelId,
      status,
      limit: limit ? Number(limit) : undefined,
      userId: req?.user.userId,
      role: req?.user.role,
    });
  }

  @Permissions('videos:upload')
  @Post()
  create(@Body() dto: CreateMediaAssetDto) {
    return this.mediaAssetService.create(dto);
  }

  @Permissions('videos:update')
  @Patch(':id/status')
  updateStatus(@Param('id') id: string, @Body() dto: UpdateMediaAssetStatusDto) {
    return this.mediaAssetService.updateStatus(id, dto);
  }

  @Permissions('videos:update')
  @Patch(':id/relay-uploaded')
  markRelayUploaded(@Param('id') id: string, @Body() dto: MarkRelayUploadedDto) {
    return this.mediaAssetService.markRelayUploaded(id, dto);
  }

  @Permissions('videos:upload')
  @Post('relay-upload/batch-enqueue')
  batchEnqueueRelayUpload(@Body() dto: BatchEnqueueRelayUploadDto) {
    return this.mediaAssetService.batchEnqueueRelayUpload(dto);
  }
}

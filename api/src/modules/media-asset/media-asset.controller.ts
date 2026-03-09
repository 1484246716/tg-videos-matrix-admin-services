import { Body, Controller, Get, Param, Patch, Post, Query } from '@nestjs/common';
import { MediaStatus } from '@prisma/client';
import { MediaAssetService } from './media-asset.service';
import { CreateMediaAssetDto } from './dto/create-media-asset.dto';
import { UpdateMediaAssetStatusDto } from './dto/update-media-asset-status.dto';
import { MarkRelayUploadedDto } from './dto/mark-relay-uploaded.dto';

@Controller('media-assets')
export class MediaAssetController {
  constructor(private readonly mediaAssetService: MediaAssetService) {}

  @Get()
  list(
    @Query('channelId') channelId?: string,
    @Query('status') status?: MediaStatus,
    @Query('limit') limit?: string,
  ) {
    return this.mediaAssetService.list({
      channelId,
      status,
      limit: limit ? Number(limit) : undefined,
    });
  }

  @Post()
  create(@Body() dto: CreateMediaAssetDto) {
    return this.mediaAssetService.create(dto);
  }

  @Patch(':id/status')
  updateStatus(@Param('id') id: string, @Body() dto: UpdateMediaAssetStatusDto) {
    return this.mediaAssetService.updateStatus(id, dto);
  }

  @Patch(':id/relay-uploaded')
  markRelayUploaded(@Param('id') id: string, @Body() dto: MarkRelayUploadedDto) {
    return this.mediaAssetService.markRelayUploaded(id, dto);
  }
}

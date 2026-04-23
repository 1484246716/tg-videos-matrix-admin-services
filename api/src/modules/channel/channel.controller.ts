import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  Query,
  Request,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { ChannelService } from './channel.service';
import { CreateChannelDto } from './dto/create-channel.dto';
import { UpdateChannelDto } from './dto/update-channel.dto';
import { UpdateChannelStatusDto } from './dto/update-channel-status.dto';
import { UpdateChannelDefaultTaxonomyDto } from './dto/update-channel-default-taxonomy.dto';
import { Prisma } from '@prisma/client';

interface AuthRequest {
  user: { userId: string; username: string; role: string };
}

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('channels')
export class ChannelController {
  constructor(private readonly channelService: ChannelService) {}

  @Permissions('channels:view')
  @Get()
  list(
    @Request() req: AuthRequest,
    @Query('status') status?: string,
    @Query('keyword') keyword?: string,
  ) {
    return this.channelService.list(req.user.userId, req.user.role, {
      status,
      keyword,
    });
  }

  @Permissions('channels:view')
  @Get(':id')
  getOne(@Param('id') id: string, @Request() req: AuthRequest) {
    return this.channelService.getOne(id, req.user.userId, req.user.role);
  }

  @Permissions('channels:update')
  @Post()
  create(@Body() dto: CreateChannelDto, @Request() req: AuthRequest) {
    return this.channelService.create(dto, req.user.userId, req.user.role);
  }

  @Permissions('channels:update')
  @Patch('batch')
  batchUpdate(
    @Body()
    body: {
      ids: string[];
      data: {
        postIntervalSec?: number;
        navIntervalSec?: number;
        navPagingEnabled?: boolean;
        navPageSize?: number;
        navEnabled?: boolean;
        defaultBotId?: string | null;
        aiSystemPromptTemplate?: string;
        cloneUseAiPromptTemplate?: boolean;
        navTemplateText?: string;
        aiReplyMarkup?: Prisma.InputJsonValue;
        navReplyMarkup?: Prisma.InputJsonValue;
        tags?: string[];
      };
    },
    @Request() req: AuthRequest,
  ) {
    return this.channelService.batchUpdate(body.ids, body.data, req.user.userId, req.user.role);
  }

  @Permissions('channels:view')
  @Get(':id/catalog-preview')
  getCatalogPreview(
    @Param('id') id: string,
    @Request() req: AuthRequest,
    @Query('page') page?: string,
    @Query('pageSize') pageSize?: string,
  ) {
    return this.channelService.getCatalogPreview(id, req.user.userId, req.user.role, {
      page,
      pageSize,
    });
  }

  @Permissions('channels:update')
  @Patch(':id/catalog-title')
  updateCatalogTitle(
    @Param('id') id: string,
    @Body() body: { mediaAssetId?: string; title?: string },
    @Request() req: AuthRequest,
  ) {
    return this.channelService.updateCatalogTitle(id, body, req.user.userId, req.user.role);
  }

  @Permissions('channels:view')
  @Get(':id/default-taxonomy')
  getDefaultTaxonomy(@Param('id') id: string, @Request() req: AuthRequest) {
    return this.channelService.getDefaultTaxonomy(id, req.user.userId, req.user.role);
  }

  @Permissions('channels:update')
  @Patch(':id/default-taxonomy')
  updateDefaultTaxonomy(
    @Param('id') id: string,
    @Body() dto: UpdateChannelDefaultTaxonomyDto,
    @Request() req: AuthRequest,
  ) {
    return this.channelService.updateDefaultTaxonomy(id, dto, req.user.userId, req.user.role);
  }

  @Permissions('channels:update')
  @Patch(':id')
  update(
    @Param('id') id: string,
    @Body() dto: UpdateChannelDto,
    @Request() req: AuthRequest,
  ) {
    return this.channelService.update(id, dto, req.user.userId, req.user.role);
  }

  @Permissions('channels:update')
  @Patch(':id/status')
  updateStatus(
    @Param('id') id: string,
    @Body() dto: UpdateChannelStatusDto,
    @Request() req: AuthRequest,
  ) {
    return this.channelService.updateStatus(id, dto, req.user.userId, req.user.role);
  }

  @Permissions('channels:update')
  @Post(':id/catalog-repair')
  repairCatalog(@Param('id') id: string, @Request() req: AuthRequest) {
    return this.channelService.repairCatalog(id, req.user.userId, req.user.role);
  }

  @Permissions('channels:delete')
  @Delete(':id')
  remove(@Param('id') id: string, @Request() req: AuthRequest) {
    return this.channelService.remove(id, req.user.userId, req.user.role);
  }
}

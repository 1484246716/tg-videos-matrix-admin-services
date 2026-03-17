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
        navEnabled?: boolean;
        defaultBotId?: string | null;
        aiSystemPromptTemplate?: string;
        navTemplateText?: string;
        aiReplyMarkup?: unknown;
        navReplyMarkup?: unknown;
        tags?: string[];
      };
    },
    @Request() req: AuthRequest,
  ) {
    return this.channelService.batchUpdate(body.ids, body.data, req.user.userId, req.user.role);
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

  @Permissions('channels:delete')
  @Delete(':id')
  remove(@Param('id') id: string, @Request() req: AuthRequest) {
    return this.channelService.remove(id, req.user.userId, req.user.role);
  }
}

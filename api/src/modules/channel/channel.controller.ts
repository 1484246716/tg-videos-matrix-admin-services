import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { ChannelService } from './channel.service';
import { CreateChannelDto } from './dto/create-channel.dto';
import { UpdateChannelDto } from './dto/update-channel.dto';
import { UpdateChannelStatusDto } from './dto/update-channel-status.dto';

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('channels')
export class ChannelController {
  constructor(private readonly channelService: ChannelService) {}

  @Permissions('videos:view')
  @Get()
  list() {
    return this.channelService.list();
  }

  @Permissions('videos:view')
  @Get(':id')
  getOne(@Param('id') id: string) {
    return this.channelService.getOne(id);
  }

  @Permissions('videos:update')
  @Post()
  create(@Body() dto: CreateChannelDto) {
    return this.channelService.create(dto);
  }

  @Permissions('videos:update')
  @Patch(':id')
  update(@Param('id') id: string, @Body() dto: UpdateChannelDto) {
    return this.channelService.update(id, dto);
  }

  @Permissions('videos:update')
  @Patch(':id/status')
  updateStatus(@Param('id') id: string, @Body() dto: UpdateChannelStatusDto) {
    return this.channelService.updateStatus(id, dto);
  }

  @Permissions('videos:delete')
  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.channelService.remove(id);
  }
}

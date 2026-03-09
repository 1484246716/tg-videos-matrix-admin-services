import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
} from '@nestjs/common';
import { ChannelService } from './channel.service';
import { CreateChannelDto } from './dto/create-channel.dto';
import { UpdateChannelDto } from './dto/update-channel.dto';
import { UpdateChannelStatusDto } from './dto/update-channel-status.dto';

@Controller('channels')
export class ChannelController {
  constructor(private readonly channelService: ChannelService) {}

  @Get()
  list() {
    return this.channelService.list();
  }

  @Get(':id')
  getOne(@Param('id') id: string) {
    return this.channelService.getOne(id);
  }

  @Post()
  create(@Body() dto: CreateChannelDto) {
    return this.channelService.create(dto);
  }

  @Patch(':id')
  update(@Param('id') id: string, @Body() dto: UpdateChannelDto) {
    return this.channelService.update(id, dto);
  }

  @Patch(':id/status')
  updateStatus(@Param('id') id: string, @Body() dto: UpdateChannelStatusDto) {
    return this.channelService.updateStatus(id, dto);
  }

  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.channelService.remove(id);
  }
}

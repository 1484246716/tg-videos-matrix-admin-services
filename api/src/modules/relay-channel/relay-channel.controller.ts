import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
} from '@nestjs/common';
import { RelayChannelService } from './relay-channel.service';
import { CreateRelayChannelDto } from './dto/create-relay-channel.dto';
import { UpdateRelayChannelDto } from './dto/update-relay-channel.dto';

@Controller('relay-channels')
export class RelayChannelController {
  constructor(private readonly relayChannelService: RelayChannelService) {}

  @Get()
  list() {
    return this.relayChannelService.list();
  }

  @Get(':id')
  getOne(@Param('id') id: string) {
    return this.relayChannelService.getOne(id);
  }

  @Post()
  create(@Body() dto: CreateRelayChannelDto) {
    return this.relayChannelService.create(dto);
  }

  @Patch(':id')
  update(@Param('id') id: string, @Body() dto: UpdateRelayChannelDto) {
    return this.relayChannelService.update(id, dto);
  }

  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.relayChannelService.remove(id);
  }
}

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
import { RelayChannelService } from './relay-channel.service';
import { CreateRelayChannelDto } from './dto/create-relay-channel.dto';
import { UpdateRelayChannelDto } from './dto/update-relay-channel.dto';

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('relay-channels')
export class RelayChannelController {
  constructor(private readonly relayChannelService: RelayChannelService) {}

  @Permissions('videos:view')
  @Get()
  list() {
    return this.relayChannelService.list();
  }

  @Permissions('videos:view')
  @Get(':id')
  getOne(@Param('id') id: string) {
    return this.relayChannelService.getOne(id);
  }

  @Permissions('videos:update')
  @Post()
  create(@Body() dto: CreateRelayChannelDto) {
    return this.relayChannelService.create(dto);
  }

  @Permissions('videos:update')
  @Patch(':id')
  update(@Param('id') id: string, @Body() dto: UpdateRelayChannelDto) {
    return this.relayChannelService.update(id, dto);
  }

  @Permissions('videos:delete')
  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.relayChannelService.remove(id);
  }
}

import {
  Body,
  Controller,
  Get,
  Param,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { CreateMassMessageCampaignDto } from './dto/create-mass-message-campaign.dto';
import { MassMessageCampaignService } from './mass-message-campaign.service';

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('mass-message-campaigns')
export class MassMessageCampaignController {
  constructor(private readonly service: MassMessageCampaignService) {}

  @Permissions('tasks:view')
  @Get()
  list(@Query('status') status?: string, @Query('limit') limit?: string) {
    return this.service.list({ status, limit: limit ? Number(limit) : undefined });
  }

  @Permissions('tasks:view')
  @Get(':id')
  getOne(@Param('id') id: string) {
    return this.service.getOne(id);
  }

  @Permissions('tasks:create')
  @Post()
  create(@Body() dto: CreateMassMessageCampaignDto) {
    return this.service.create(dto);
  }
}


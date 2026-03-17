import {
  Body,
  Controller,
  Get,
  Param,
  Post,
  Query,
  Request,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { CreateMassMessageCampaignDto } from './dto/create-mass-message-campaign.dto';
import { MassMessageCampaignService } from './mass-message-campaign.service';

interface AuthRequest {
  user: { userId: string; username: string; role: string };
}

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('mass-message-campaigns')
export class MassMessageCampaignController {
  constructor(private readonly service: MassMessageCampaignService) {}

  @Permissions('mass-messaging:view')
  @Get()
  list(
    @Query('status') status?: string,
    @Query('limit') limit?: string,
    @Request() req?: AuthRequest,
  ) {
    return this.service.list({
      status,
      limit: limit ? Number(limit) : undefined,
      userId: req?.user.userId,
      role: req?.user.role,
    });
  }

  @Permissions('mass-messaging:view')
  @Get(':id')
  getOne(@Param('id') id: string, @Request() req?: AuthRequest) {
    return this.service.getOne(id, req?.user.userId, req?.user.role);
  }

  @Permissions('mass-messaging:create')
  @Post()
  create(@Body() dto: CreateMassMessageCampaignDto, @Request() req?: AuthRequest) {
    return this.service.create(dto, req?.user.userId, req?.user.role);
  }
}


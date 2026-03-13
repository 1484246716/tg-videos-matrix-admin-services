import { Controller, Get, Query, Request, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { MassMessageItemService } from './mass-message-item.service';

interface AuthRequest {
  user: { userId: string; username: string; role: string };
}

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('mass-message-items')
export class MassMessageItemController {
  constructor(private readonly service: MassMessageItemService) {}

  @Permissions('tasks:view')
  @Get()
  list(
    @Query('campaignId') campaignId?: string,
    @Query('status') status?: string,
    @Query('limit') limit?: string,
    @Query('offset') offset?: string,
    @Request() req?: AuthRequest,
  ) {
    return this.service.list({
      campaignId,
      status,
      limit: limit ? Number(limit) : undefined,
      offset: offset ? Number(offset) : undefined,
      userId: req?.user.userId,
      role: req?.user.role,
    });
  }
}


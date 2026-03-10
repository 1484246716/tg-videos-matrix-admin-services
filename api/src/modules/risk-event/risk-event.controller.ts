import { Controller, Get, Query, UseGuards } from '@nestjs/common';
import { RiskLevel } from '@prisma/client';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { RiskEventService } from './risk-event.service';

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('risk-events')
export class RiskEventController {
  constructor(private readonly riskEventService: RiskEventService) {}

  @Permissions('tasks:view')
  @Get()
  list(
    @Query('eventType') eventType?: string,
    @Query('level') level?: RiskLevel,
    @Query('channelId') channelId?: string,
    @Query('botId') botId?: string,
    @Query('dispatchTaskId') dispatchTaskId?: string,
    @Query('from') from?: string,
    @Query('to') to?: string,
    @Query('limit') limit?: string,
  ) {
    return this.riskEventService.list({
      eventType,
      level,
      channelId,
      botId,
      dispatchTaskId,
      from,
      to,
      limit: limit ? Number(limit) : undefined,
    });
  }
}

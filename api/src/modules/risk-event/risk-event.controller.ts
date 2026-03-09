import { Controller, Get, Query } from '@nestjs/common';
import { RiskLevel } from '@prisma/client';
import { RiskEventService } from './risk-event.service';

@Controller('risk-events')
export class RiskEventController {
  constructor(private readonly riskEventService: RiskEventService) {}

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

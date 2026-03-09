import { Module } from '@nestjs/common';
import { RiskEventController } from './risk-event.controller';
import { RiskEventService } from './risk-event.service';

@Module({
  controllers: [RiskEventController],
  providers: [RiskEventService],
})
export class RiskEventModule {}

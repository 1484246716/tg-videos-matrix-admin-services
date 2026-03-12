import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { MassMessageCampaignController } from './mass-message-campaign.controller';
import { MassMessageCampaignService } from './mass-message-campaign.service';

@Module({
  imports: [PrismaModule],
  controllers: [MassMessageCampaignController],
  providers: [MassMessageCampaignService],
})
export class MassMessageCampaignModule {}


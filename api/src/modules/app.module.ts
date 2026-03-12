import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { HealthModule } from './health/health.module';
import { ChannelModule } from './channel/channel.module';
import { BotModule } from './bot/bot.module';
import { PrismaModule } from './prisma/prisma.module';
import { RelayChannelModule } from './relay-channel/relay-channel.module';
import { MediaAssetModule } from './media-asset/media-asset.module';
import { DispatchModule } from './dispatch/dispatch.module';
import { RiskEventModule } from './risk-event/risk-event.module';
import { CatalogModule } from './catalog/catalog.module';
import { TaskDefinitionModule } from './task-definition/task-definition.module';
import { MediaLifecycleModule } from './media-lifecycle/media-lifecycle.module';
import { AuthModule } from './auth/auth.module';
import { UserModule } from './user/user.module';
import { MessageTemplateModule } from './message-template/message-template.module';
import { MassMessageCampaignModule } from './mass-message-campaign/mass-message-campaign.module';
import { MassMessageItemModule } from './mass-message-item/mass-message-item.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: ['../.env', '.env'],
    }),
    PrismaModule,
    HealthModule,
    ChannelModule,
    BotModule,
    RelayChannelModule,
    MediaAssetModule,
    DispatchModule,
    RiskEventModule,
    CatalogModule,
    TaskDefinitionModule,
    MediaLifecycleModule,
    AuthModule,
    UserModule,
    MessageTemplateModule,
    MassMessageCampaignModule,
    MassMessageItemModule,
  ],
})
export class AppModule { }

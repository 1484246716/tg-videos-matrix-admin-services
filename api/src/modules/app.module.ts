import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { HealthModule } from './health/health.module';
import { ChannelModule } from './channel/channel.module';
import { BotModule } from './bot/bot.module';
import { AiModelModule } from './ai-model/ai-model.module';
import { PrismaModule } from './prisma/prisma.module';
import { RelayChannelModule } from './relay-channel/relay-channel.module';
import { MediaAssetModule } from './media-asset/media-asset.module';
import { DispatchModule } from './dispatch/dispatch.module';
import { RiskEventModule } from './risk-event/risk-event.module';
import { CatalogModule } from './catalog/catalog.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: ['.env', '../../.env'],
    }),
    PrismaModule,
    HealthModule,
    ChannelModule,
    BotModule,
    AiModelModule,
    RelayChannelModule,
    MediaAssetModule,
    DispatchModule,
    RiskEventModule,
    CatalogModule,
  ],
})
export class AppModule {}

import { Module } from '@nestjs/common';
import { RelayChannelController } from './relay-channel.controller';
import { RelayChannelService } from './relay-channel.service';

@Module({
  controllers: [RelayChannelController],
  providers: [RelayChannelService],
})
export class RelayChannelModule {}

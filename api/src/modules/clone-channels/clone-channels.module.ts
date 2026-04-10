import { Module } from '@nestjs/common';
import { CloneChannelsController } from './clone-channels.controller';
import { CloneChannelsService } from './clone-channels.service';

@Module({
  controllers: [CloneChannelsController],
  providers: [CloneChannelsService],
})
export class CloneChannelsModule {}

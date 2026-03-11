import { Module } from '@nestjs/common';
import { MediaLifecycleController } from './media-lifecycle.controller';
import { MediaLifecycleService } from './media-lifecycle.service';

@Module({
  controllers: [MediaLifecycleController],
  providers: [MediaLifecycleService],
})
export class MediaLifecycleModule {}

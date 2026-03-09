import { Module } from '@nestjs/common';
import { MediaAssetController } from './media-asset.controller';
import { MediaAssetService } from './media-asset.service';

@Module({
  controllers: [MediaAssetController],
  providers: [MediaAssetService],
})
export class MediaAssetModule {}

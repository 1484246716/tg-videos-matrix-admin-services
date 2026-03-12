import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { MassMessageItemController } from './mass-message-item.controller';
import { MassMessageItemService } from './mass-message-item.service';

@Module({
  imports: [PrismaModule],
  controllers: [MassMessageItemController],
  providers: [MassMessageItemService],
})
export class MassMessageItemModule {}


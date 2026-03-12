import { Module } from '@nestjs/common';
import { PrismaModule } from '../prisma/prisma.module';
import { MessageTemplateController } from './message-template.controller';
import { MessageTemplateService } from './message-template.service';

@Module({
  imports: [PrismaModule],
  controllers: [MessageTemplateController],
  providers: [MessageTemplateService],
})
export class MessageTemplateModule {}


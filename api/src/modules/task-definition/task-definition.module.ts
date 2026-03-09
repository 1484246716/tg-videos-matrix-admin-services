import { Module } from '@nestjs/common';
import { TaskDefinitionController } from './task-definition.controller';
import { TaskDefinitionService } from './task-definition.service';

@Module({
  controllers: [TaskDefinitionController],
  providers: [TaskDefinitionService],
})
export class TaskDefinitionModule {}

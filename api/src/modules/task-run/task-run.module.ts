import { Module } from '@nestjs/common';
import { TaskRunController } from './task-run.controller';
import { TaskRunService } from './task-run.service';

@Module({
  controllers: [TaskRunController],
  providers: [TaskRunService],
})
export class TaskRunModule {}

import { Controller, Get, Param, Query, UseGuards } from '@nestjs/common';
import { TaskRunEntityType, TaskRunStatus, TaskRunStepStatus, TaskDefinitionType } from '@prisma/client';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { TaskRunService } from './task-run.service';

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('task-runs')
export class TaskRunController {
  constructor(private readonly taskRunService: TaskRunService) {}

  @Permissions('tasks:view')
  @Get()
  list(
    @Query('taskDefinitionId') taskDefinitionId?: string,
    @Query('taskType') taskType?: TaskDefinitionType,
    @Query('status') status?: TaskRunStatus,
    @Query('limit') limit?: string,
  ) {
    return this.taskRunService.list({
      taskDefinitionId,
      taskType,
      status,
      limit: limit ? Number(limit) : undefined,
    });
  }

  @Permissions('tasks:view')
  @Get(':id')
  getById(@Param('id') id: string) {
    return this.taskRunService.getById(id);
  }

  @Permissions('tasks:view')
  @Get(':id/steps')
  steps(
    @Param('id') id: string,
    @Query('status') status?: TaskRunStepStatus,
    @Query('entityType') entityType?: TaskRunEntityType,
    @Query('limit') limit?: string,
  ) {
    return this.taskRunService.listSteps(id, {
      status,
      entityType,
      limit: limit ? Number(limit) : undefined,
    });
  }
}

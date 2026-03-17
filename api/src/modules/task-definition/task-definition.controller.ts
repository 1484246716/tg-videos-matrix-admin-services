import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { CreateTaskDefinitionDto } from './dto/create-task-definition.dto';
import { UpdateTaskDefinitionDto } from './dto/update-task-definition.dto';
import { TaskDefinitionService } from './task-definition.service';

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('task-definitions')
export class TaskDefinitionController {
  constructor(private readonly taskDefinitionService: TaskDefinitionService) {}

  @Permissions("tasks:view")
  @Get()
  list(
    @Query('taskType') taskType?:
      | 'relay_upload'
      | 'dispatch_send'
      | 'catalog_publish'
      | 'mass_message',
    @Query('isEnabled') isEnabled?: string,
    @Query('limit') limit?: string,
  ) {
    return this.taskDefinitionService.list({
      taskType,
      isEnabled,
      limit: limit ? Number(limit) : undefined,
    });
  }

  @Permissions('tasks:create')
  @Post()
  create(@Body() dto: CreateTaskDefinitionDto) {
    return this.taskDefinitionService.create(dto);
  }

  @Permissions('tasks:update')
  @Patch(':id')
  update(@Param('id') id: string, @Body() dto: UpdateTaskDefinitionDto) {
    return this.taskDefinitionService.update(id, dto);
  }

  @Permissions('tasks:update')
  @Patch(':id/toggle')
  toggle(@Param('id') id: string, @Body('isEnabled') isEnabled: boolean) {
    return this.taskDefinitionService.toggle(id, isEnabled);
  }

  @Permissions('tasks:delete')
  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.taskDefinitionService.remove(id);
  }
}

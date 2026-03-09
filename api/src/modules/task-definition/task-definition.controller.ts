import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  Query,
} from '@nestjs/common';
import { CreateTaskDefinitionDto } from './dto/create-task-definition.dto';
import { UpdateTaskDefinitionDto } from './dto/update-task-definition.dto';
import { TaskDefinitionService } from './task-definition.service';

@Controller('task-definitions')
export class TaskDefinitionController {
  constructor(private readonly taskDefinitionService: TaskDefinitionService) {}

  @Get()
  list(
    @Query('taskType') taskType?: 'relay_upload' | 'dispatch_send' | 'catalog_publish',
    @Query('isEnabled') isEnabled?: string,
    @Query('limit') limit?: string,
  ) {
    return this.taskDefinitionService.list({
      taskType,
      isEnabled,
      limit: limit ? Number(limit) : undefined,
    });
  }

  @Post()
  create(@Body() dto: CreateTaskDefinitionDto) {
    return this.taskDefinitionService.create(dto);
  }

  @Patch(':id')
  update(@Param('id') id: string, @Body() dto: UpdateTaskDefinitionDto) {
    return this.taskDefinitionService.update(id, dto);
  }

  @Patch(':id/toggle')
  toggle(@Param('id') id: string, @Body('isEnabled') isEnabled: boolean) {
    return this.taskDefinitionService.toggle(id, isEnabled);
  }

  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.taskDefinitionService.remove(id);
  }
}

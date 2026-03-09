import { Body, Controller, Get, Param, Patch, Post, Query } from '@nestjs/common';
import { TaskStatus } from '@prisma/client';
import { DispatchService } from './dispatch.service';
import { CreateDispatchTaskDto } from './dto/create-dispatch-task.dto';
import { UpdateDispatchTaskStatusDto } from './dto/update-dispatch-task-status.dto';

@Controller('dispatch-tasks')
export class DispatchController {
  constructor(private readonly dispatchService: DispatchService) {}

  @Get()
  list(
    @Query('channelId') channelId?: string,
    @Query('status') status?: TaskStatus,
    @Query('limit') limit?: string,
  ) {
    return this.dispatchService.list({
      channelId,
      status,
      limit: limit ? Number(limit) : undefined,
    });
  }

  @Post()
  create(@Body() dto: CreateDispatchTaskDto) {
    return this.dispatchService.create(dto);
  }

  @Patch(':id/status')
  updateStatus(
    @Param('id') id: string,
    @Body() dto: UpdateDispatchTaskStatusDto,
  ) {
    return this.dispatchService.updateStatus(id, dto);
  }

  @Get(':id/logs')
  logs(@Param('id') id: string, @Query('limit') limit?: string) {
    return this.dispatchService.listLogs(id, limit ? Number(limit) : undefined);
  }
}

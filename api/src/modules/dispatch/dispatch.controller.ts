import { Body, Controller, Get, Param, Patch, Post, Query, UseGuards } from '@nestjs/common';
import { TaskStatus } from '@prisma/client';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { DispatchService } from './dispatch.service';
import { CreateDispatchTaskDto } from './dto/create-dispatch-task.dto';
import { UpdateDispatchTaskStatusDto } from './dto/update-dispatch-task-status.dto';

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('dispatch-tasks')
export class DispatchController {
  constructor(private readonly dispatchService: DispatchService) {}

  @Permissions("tasks:view")
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

  @Permissions("tasks:create")
  @Post()
  create(@Body() dto: CreateDispatchTaskDto) {
    return this.dispatchService.create(dto);
  }

  @Permissions("tasks:update")
  @Patch(':id/status')
  updateStatus(
    @Param('id') id: string,
    @Body() dto: UpdateDispatchTaskStatusDto,
  ) {
    return this.dispatchService.updateStatus(id, dto);
  }

  @Permissions("tasks:view")
  @Get(':id/logs')
  logs(@Param('id') id: string, @Query('limit') limit?: string) {
    return this.dispatchService.listLogs(id, limit ? Number(limit) : undefined);
  }
}

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
import { CreateMessageTemplateDto } from './dto/create-message-template.dto';
import { UpdateMessageTemplateDto } from './dto/update-message-template.dto';
import { MessageTemplateService } from './message-template.service';

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('message-templates')
export class MessageTemplateController {
  constructor(private readonly service: MessageTemplateService) {}

  @Permissions('tasks:view')
  @Get()
  list(@Query('isActive') isActive?: string, @Query('limit') limit?: string) {
    return this.service.list({ isActive, limit: limit ? Number(limit) : undefined });
  }

  @Permissions('tasks:view')
  @Get(':id')
  getOne(@Param('id') id: string) {
    return this.service.getOne(id);
  }

  @Permissions('tasks:create')
  @Post()
  create(@Body() dto: CreateMessageTemplateDto) {
    return this.service.create(dto);
  }

  @Permissions('tasks:update')
  @Patch(':id')
  update(@Param('id') id: string, @Body() dto: UpdateMessageTemplateDto) {
    return this.service.update(id, dto);
  }

  @Permissions('tasks:delete')
  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.service.remove(id);
  }
}


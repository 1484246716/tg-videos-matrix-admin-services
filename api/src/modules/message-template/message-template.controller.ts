import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  Query,
  Request,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { CreateMessageTemplateDto } from './dto/create-message-template.dto';
import { UpdateMessageTemplateDto } from './dto/update-message-template.dto';
import { MessageTemplateService } from './message-template.service';

interface AuthRequest {
  user: { userId: string; username: string; role: string };
}

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('message-templates')
export class MessageTemplateController {
  constructor(private readonly service: MessageTemplateService) {}

  @Permissions('tasks:view')
  @Get()
  list(
    @Query('isActive') isActive?: string,
    @Query('limit') limit?: string,
    @Request() req?: AuthRequest,
  ) {
    return this.service.list({
      isActive,
      limit: limit ? Number(limit) : undefined,
      userId: req?.user.userId,
      role: req?.user.role,
    });
  }

  @Permissions('tasks:view')
  @Get(':id')
  getOne(@Param('id') id: string, @Request() req?: AuthRequest) {
    return this.service.getOne(id, req?.user.userId, req?.user.role);
  }

  @Permissions('tasks:create')
  @Post()
  create(@Body() dto: CreateMessageTemplateDto, @Request() req?: AuthRequest) {
    return this.service.create(dto, req?.user.userId, req?.user.role);
  }

  @Permissions('tasks:update')
  @Patch(':id')
  update(
    @Param('id') id: string,
    @Body() dto: UpdateMessageTemplateDto,
    @Request() req?: AuthRequest,
  ) {
    return this.service.update(id, dto, req?.user.userId, req?.user.role);
  }

  @Permissions('tasks:delete')
  @Delete(':id')
  remove(@Param('id') id: string, @Request() req?: AuthRequest) {
    return this.service.remove(id, req?.user.userId, req?.user.role);
  }
}


import { Body, Controller, Delete, Get, Param, Patch, Post, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { BotService } from './bot.service';
import { CreateBotDto } from './dto/create-bot.dto';
import { UpdateBotDto } from './dto/update-bot.dto';

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('bots')
export class BotController {
  constructor(private readonly botService: BotService) {}

  @Permissions('tasks:view')
  @Get()
  list() {
    return this.botService.list();
  }

  @Permissions('tasks:create')
  @Post()
  create(@Body() dto: CreateBotDto) {
    return this.botService.create(dto);
  }

  @Permissions('tasks:update')
  @Patch(':id')
  update(@Param('id') id: string, @Body() dto: UpdateBotDto) {
    return this.botService.update(id, dto);
  }

  @Permissions('tasks:delete')
  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.botService.remove(id);
  }
}

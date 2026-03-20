import { Body, Controller, Delete, Get, Param, Patch, Post, Query, UseGuards } from '@nestjs/common';
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

  @Permissions('bots:view')
  @Get()
  list() {
    return this.botService.list();
  }

  @Permissions('bots:create')
  @Post()
  create(@Body() dto: CreateBotDto) {
    return this.botService.create(dto);
  }

  @Permissions('bots:update')
  @Patch(':id')
  update(@Param('id') id: string, @Body() dto: UpdateBotDto) {
    return this.botService.update(id, dto);
  }

  @Permissions('bots:delete')
  @Delete(':id')
  remove(@Param('id') id: string, @Query('force') force?: string) {
    return this.botService.remove(id, force === 'true');
  }
}

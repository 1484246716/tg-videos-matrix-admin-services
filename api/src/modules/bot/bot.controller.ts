import { Body, Controller, Delete, Get, Param, Patch, Post } from '@nestjs/common';
import { BotService } from './bot.service';
import { CreateBotDto } from './dto/create-bot.dto';
import { UpdateBotDto } from './dto/update-bot.dto';

@Controller('bots')
export class BotController {
  constructor(private readonly botService: BotService) {}

  @Get()
  list() {
    return this.botService.list();
  }

  @Post()
  create(@Body() dto: CreateBotDto) {
    return this.botService.create(dto);
  }

  @Patch(':id')
  update(@Param('id') id: string, @Body() dto: UpdateBotDto) {
    return this.botService.update(id, dto);
  }

  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.botService.remove(id);
  }
}

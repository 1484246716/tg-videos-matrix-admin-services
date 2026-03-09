import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
} from '@nestjs/common';
import { AiModelService } from './ai-model.service';
import { CreateAiModelDto } from './dto/create-ai-model.dto';
import { UpdateAiModelDto } from './dto/update-ai-model.dto';

@Controller('ai-model-profiles')
export class AiModelController {
  constructor(private readonly aiModelService: AiModelService) {}

  @Get()
  list() {
    return this.aiModelService.list();
  }

  @Get(':id')
  getOne(@Param('id') id: string) {
    return this.aiModelService.getOne(id);
  }

  @Post()
  create(@Body() dto: CreateAiModelDto) {
    return this.aiModelService.create(dto);
  }

  @Patch(':id')
  update(@Param('id') id: string, @Body() dto: UpdateAiModelDto) {
    return this.aiModelService.update(id, dto);
  }

  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.aiModelService.remove(id);
  }
}

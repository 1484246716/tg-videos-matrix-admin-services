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
import { CatalogTaskStatus } from '@prisma/client';
import { CatalogService } from './catalog.service';
import { CreateCatalogTemplateDto } from './dto/create-catalog-template.dto';
import { UpdateCatalogTemplateDto } from './dto/update-catalog-template.dto';
import { RenderCatalogPreviewDto } from './dto/render-catalog-preview.dto';
import { PublishCatalogDto } from './dto/publish-catalog.dto';

@Controller('catalogs')
export class CatalogController {
  constructor(private readonly catalogService: CatalogService) {}

  @Get('templates')
  listTemplates() {
    return this.catalogService.listTemplates();
  }

  @Get('templates/:id')
  getTemplate(@Param('id') id: string) {
    return this.catalogService.getTemplate(id);
  }

  @Post('templates')
  createTemplate(@Body() dto: CreateCatalogTemplateDto) {
    return this.catalogService.createTemplate(dto);
  }

  @Patch('templates/:id')
  updateTemplate(@Param('id') id: string, @Body() dto: UpdateCatalogTemplateDto) {
    return this.catalogService.updateTemplate(id, dto);
  }

  @Delete('templates/:id')
  deleteTemplate(@Param('id') id: string) {
    return this.catalogService.deleteTemplate(id);
  }

  @Post('render-preview')
  renderPreview(@Body() dto: RenderCatalogPreviewDto) {
    return this.catalogService.renderPreview(dto);
  }

  @Post('publish')
  publish(@Body() dto: PublishCatalogDto) {
    return this.catalogService.publish(dto);
  }

  @Get('tasks')
  listTasks(
    @Query('channelId') channelId?: string,
    @Query('status') status?: CatalogTaskStatus,
    @Query('limit') limit?: string,
  ) {
    return this.catalogService.listTasks(
      channelId,
      status,
      limit ? Number(limit) : undefined,
    );
  }

  @Get('histories')
  listHistories(
    @Query('channelId') channelId?: string,
    @Query('catalogTemplateId') catalogTemplateId?: string,
    @Query('limit') limit?: string,
  ) {
    return this.catalogService.listHistories({
      channelId,
      catalogTemplateId,
      limit: limit ? Number(limit) : undefined,
    });
  }
}

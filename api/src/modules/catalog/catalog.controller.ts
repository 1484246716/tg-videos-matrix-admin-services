import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Patch,
  Post,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { CatalogService } from './catalog.service';
import { CreateCatalogTemplateDto } from './dto/create-catalog-template.dto';
import { UpdateCatalogTemplateDto } from './dto/update-catalog-template.dto';
import { RenderCatalogPreviewDto } from './dto/render-catalog-preview.dto';
import { PublishCatalogDto } from './dto/publish-catalog.dto';

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('catalogs')
export class CatalogController {
  constructor(private readonly catalogService: CatalogService) {}

  @Permissions('tasks:view')
  @Get('templates')
  listTemplates() {
    return this.catalogService.listTemplates();
  }

  @Permissions('tasks:view')
  @Get('templates/:id')
  getTemplate(@Param('id') id: string) {
    return this.catalogService.getTemplate(id);
  }

  @Permissions('tasks:create')
  @Post('templates')
  createTemplate(@Body() dto: CreateCatalogTemplateDto) {
    return this.catalogService.createTemplate(dto);
  }

  @Permissions('tasks:update')
  @Patch('templates/:id')
  updateTemplate(@Param('id') id: string, @Body() dto: UpdateCatalogTemplateDto) {
    return this.catalogService.updateTemplate(id, dto);
  }

  @Permissions('tasks:delete')
  @Delete('templates/:id')
  deleteTemplate(@Param('id') id: string) {
    return this.catalogService.deleteTemplate(id);
  }

  @Permissions('tasks:view')
  @Post('render-preview')
  renderPreview(@Body() dto: RenderCatalogPreviewDto) {
    return this.catalogService.renderPreview(dto);
  }

  @Permissions('tasks:create')
  @Post('publish')
  publish(@Body() dto: PublishCatalogDto) {
    return this.catalogService.publish(dto);
  }

}

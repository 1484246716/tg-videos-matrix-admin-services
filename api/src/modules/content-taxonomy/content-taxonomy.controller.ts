import { Body, Controller, Get, Param, Patch, Post, Query, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { SaveCategoryLevel1Dto } from './dto/save-category-level1.dto';
import { SaveCategoryLevel2Dto } from './dto/save-category-level2.dto';
import { SaveContentTagDto } from './dto/save-content-tag.dto';
import { UpdateCategoryLevel1Dto } from './dto/update-category-level1.dto';
import { UpdateCategoryLevel2Dto } from './dto/update-category-level2.dto';
import { UpdateContentTagDto } from './dto/update-content-tag.dto';
import { ContentTaxonomyService } from './content-taxonomy.service';

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('content-taxonomy')
export class ContentTaxonomyController {
  constructor(private readonly contentTaxonomyService: ContentTaxonomyService) {}

  @Permissions('search:view')
  @Get('level1')
  listLevel1(@Query('status') status?: string) {
    return this.contentTaxonomyService.listLevel1({ status });
  }

  @Permissions('search:view')
  @Get('level2')
  listLevel2(@Query('status') status?: string, @Query('level1Id') level1Id?: string) {
    return this.contentTaxonomyService.listLevel2({ status, level1Id });
  }

  @Permissions('search:view')
  @Get('tags')
  listTags(@Query('status') status?: string, @Query('scope') scope?: string) {
    return this.contentTaxonomyService.listTags({ status, scope });
  }

  @Permissions('search:manage')
  @Post('level1')
  createLevel1(@Body() dto: SaveCategoryLevel1Dto) {
    return this.contentTaxonomyService.createLevel1(dto);
  }

  @Permissions('search:manage')
  @Patch('level1/:id')
  updateLevel1(@Param('id') id: string, @Body() dto: UpdateCategoryLevel1Dto) {
    return this.contentTaxonomyService.updateLevel1(id, dto);
  }

  @Permissions('search:manage')
  @Post('level2')
  createLevel2(@Body() dto: SaveCategoryLevel2Dto) {
    return this.contentTaxonomyService.createLevel2(dto);
  }

  @Permissions('search:manage')
  @Patch('level2/:id')
  updateLevel2(@Param('id') id: string, @Body() dto: UpdateCategoryLevel2Dto) {
    return this.contentTaxonomyService.updateLevel2(id, dto);
  }

  @Permissions('search:manage')
  @Post('tags')
  createTag(@Body() dto: SaveContentTagDto) {
    return this.contentTaxonomyService.createTag(dto);
  }

  @Permissions('search:manage')
  @Patch('tags/:id')
  updateTag(@Param('id') id: string, @Body() dto: UpdateContentTagDto) {
    return this.contentTaxonomyService.updateTag(id, dto);
  }
}

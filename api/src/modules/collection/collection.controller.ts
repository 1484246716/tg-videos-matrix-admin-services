import { Body, Controller, Delete, Get, Param, Patch, Post, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { CollectionService } from './collection.service';

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('collections')
export class CollectionController {
  constructor(private readonly collectionService: CollectionService) {}

  @Permissions('collections:view')
  @Get()
  list() {
    return this.collectionService.list();
  }

  @Permissions('collections:create')
  @Post()
  create(@Body() dto: any) {
    return this.collectionService.create(dto);
  }

  @Permissions('collections:update')
  @Patch(':id')
  update(@Param('id') id: string, @Body() dto: any) {
    return this.collectionService.update(id, dto);
  }

  @Permissions('collections:delete')
  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.collectionService.remove(id);
  }
}

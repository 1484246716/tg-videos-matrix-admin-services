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
import { CollectionService } from './collection.service';

interface AuthRequest {
  user: { userId: string; username: string; role: string };
}

@UseGuards(JwtAuthGuard, PermissionsGuard)
@Controller('collections')
export class CollectionController {
  constructor(private readonly collectionService: CollectionService) {}

  @Permissions('collections:view')
  @Get()
  list(@Request() req: AuthRequest) {
    return this.collectionService.list(req.user.userId, req.user.role);
  }

  @Permissions('collections:create')
  @Post()
  create(@Body() dto: any, @Request() req: AuthRequest) {
    return this.collectionService.create(dto, req.user.userId, req.user.role);
  }

  @Permissions('collections:view')
  @Get(':id/catalog-preview')
  getCatalogPreview(
    @Param('id') id: string,
    @Request() req: AuthRequest,
    @Query('page') page?: string,
    @Query('pageSize') pageSize?: string,
  ) {
    return this.collectionService.getCatalogPreview(id, req.user.userId, req.user.role, {
      page,
      pageSize,
    });
  }

  @Permissions('collections:update')
  @Patch(':id/catalog-title')
  updateCatalogTitle(
    @Param('id') id: string,
    @Body() body: { episodeId?: string; title?: string },
    @Request() req: AuthRequest,
  ) {
    return this.collectionService.updateCatalogTitle(id, body, req.user.userId, req.user.role);
  }

  @Permissions('collections:update')
  @Patch(':id')
  update(@Param('id') id: string, @Body() dto: any, @Request() req: AuthRequest) {
    return this.collectionService.update(id, dto, req.user.userId, req.user.role);
  }

  @Permissions('collections:delete')
  @Delete(':id')
  remove(@Param('id') id: string, @Request() req: AuthRequest) {
    return this.collectionService.remove(id, req.user.userId, req.user.role);
  }
}

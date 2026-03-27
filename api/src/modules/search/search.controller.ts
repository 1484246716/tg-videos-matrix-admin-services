import { Controller, Get, Query, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { PermissionsGuard } from '../auth/permissions.guard';
import { Permissions } from '../auth/permissions.decorator';
import { SearchService } from './search.service';

@Controller('search')
export class SearchController {
  constructor(private readonly searchService: SearchService) {}

  /**
   * GET /api/search?keyword=xxx&channelIds=1,2&limit=20&offset=0
   * 管理后台搜索接口
   */
  @UseGuards(JwtAuthGuard, PermissionsGuard)
  @Permissions('search:view')
  @Get()
  async search(
    @Query('keyword') keyword: string,
    @Query('channelIds') channelIdsRaw?: string,
    @Query('limit') limitRaw?: string,
    @Query('offset') offsetRaw?: string,
  ) {
    const channelIds = channelIdsRaw
      ? channelIdsRaw.split(',').map(s => s.trim()).filter(Boolean)
      : undefined;

    return this.searchService.search({
      keyword,
      channelIds,
      limit: limitRaw ? parseInt(limitRaw, 10) : 20,
      offset: offsetRaw ? parseInt(offsetRaw, 10) : 0,
    });
  }

  /**
   * GET /api/search/stats
   * 搜索索引统计信息
   */
  @UseGuards(JwtAuthGuard, PermissionsGuard)
  @Permissions('search:view')
  @Get('stats')
  async stats() {
    return this.searchService.getStats();
  }
}

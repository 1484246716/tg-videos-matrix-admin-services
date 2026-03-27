import { Body, Controller, Get, Post, Query, Request, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { PermissionsGuard } from '../auth/permissions.guard';
import { Permissions } from '../auth/permissions.decorator';
import { SearchService } from './search.service';

interface AuthRequest {
  user: { userId: string; username: string; role: string; permissions?: string[] };
}

@Controller('search')
export class SearchController {
  constructor(private readonly searchService: SearchService) {}

  /**
   * GET /api/search?keyword=xxx&channelIds=1,2&limit=20&offset=0&fallbackToDb=true
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
    @Query('fallbackToDb') fallbackToDbRaw?: string,
    @Request() req?: AuthRequest,
  ) {
    const channelIds = channelIdsRaw
      ? channelIdsRaw
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean)
      : undefined;

    return this.searchService.search({
      keyword,
      channelIds,
      limit: limitRaw ? Number.parseInt(limitRaw, 10) : undefined,
      offset: offsetRaw ? Number.parseInt(offsetRaw, 10) : undefined,
      fallbackToDb: fallbackToDbRaw ? fallbackToDbRaw !== 'false' : true,
      userId: req?.user.userId,
      role: req?.user.role,
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

  /**
   * POST /api/search/outbox/process?limit=50
   * 手动触发 outbox 消费（管理接口）
   */
  @UseGuards(JwtAuthGuard, PermissionsGuard)
  @Permissions('search:manage')
  @Post('outbox/process')
  async processOutbox(@Query('limit') limitRaw?: string) {
    return this.searchService.processOutbox(limitRaw ? Number.parseInt(limitRaw, 10) : undefined);
  }

  /**
   * POST /api/search/opensearch/init
   * 初始化 OpenSearch 索引（mapping + analyzer + alias）
   */
  @UseGuards(JwtAuthGuard, PermissionsGuard)
  @Permissions('search:manage')
  @Post('opensearch/init')
  async initOpenSearch() {
    return this.searchService.initOpenSearch();
  }

  /**
   * POST /api/search/opensearch/switch
   * 切换 OpenSearch 读/写别名到目标索引
   */
  @UseGuards(JwtAuthGuard, PermissionsGuard)
  @Permissions('search:manage')
  @Post('opensearch/switch')
  async switchOpenSearch(@Body('targetIndex') targetIndex: string) {
    return this.searchService.switchOpenSearchAliases(targetIndex);
  }
}

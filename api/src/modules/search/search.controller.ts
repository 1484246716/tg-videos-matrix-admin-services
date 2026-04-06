import { Body, Controller, Get, Post, Query, Request, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { PermissionsGuard } from '../auth/permissions.guard';
import { Permissions } from '../auth/permissions.decorator';
import { SearchService } from './search.service';
import { InternalTokenGuard } from './internal-token.guard';

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
   * GET /api/search/internal?keyword=xxx&channelTgChatId=-100xxx&limit=20&offset=0&fallbackToDb=true
   * 机器人内部搜索接口（X-Internal-Token 鉴权）
   */
  @UseGuards(InternalTokenGuard)
  @Get('internal')
  async internalSearch(
    @Query('keyword') keyword: string,
    @Query('channelTgChatId') channelTgChatId?: string,
    @Query('limit') limitRaw?: string,
    @Query('offset') offsetRaw?: string,
    @Query('fallbackToDb') fallbackToDbRaw?: string,
  ) {
    const resolvedChannelIds = channelTgChatId
      ? await this.searchService.resolveChannelIdsByTgChatIds([channelTgChatId])
      : undefined;

    return this.searchService.search({
      keyword,
      channelIds: resolvedChannelIds,
      limit: limitRaw ? Number.parseInt(limitRaw, 10) : undefined,
      offset: offsetRaw ? Number.parseInt(offsetRaw, 10) : undefined,
      fallbackToDb: fallbackToDbRaw ? fallbackToDbRaw !== 'false' : true,
      role: 'admin',
    });
  }

  /**
   * GET /api/search/internal/hot?channelTgChatId=-100xxx&limit=20&offset=0&period=7d&fallbackToDb=true
   * 机器人内部热门接口（X-Internal-Token 鉴权）
   */
  @UseGuards(InternalTokenGuard)
  @Get('internal/hot')
  async internalHot(
    @Query('channelTgChatId') channelTgChatId?: string,
    @Query('limit') limitRaw?: string,
    @Query('offset') offsetRaw?: string,
    @Query('period') period?: string,
    @Query('fallbackToDb') fallbackToDbRaw?: string,
  ) {
    const resolvedChannelIds = channelTgChatId
      ? await this.searchService.resolveChannelIdsByTgChatIds([channelTgChatId])
      : undefined;

    return this.searchService.searchHot({
      channelIds: resolvedChannelIds,
      limit: limitRaw ? Number.parseInt(limitRaw, 10) : undefined,
      offset: offsetRaw ? Number.parseInt(offsetRaw, 10) : undefined,
      period,
      fallbackToDb: fallbackToDbRaw ? fallbackToDbRaw !== 'false' : true,
      role: 'admin',
    });
  }

  /**
   * GET /api/search/internal/tags?channelTgChatId=-100xxx&limit=30&offset=0
   * 机器人内部一级分类面板接口（X-Internal-Token 鉴权）
   */
  @UseGuards(InternalTokenGuard)
  @Get('internal/tags')
  async internalTags(
    @Query('channelTgChatId') channelTgChatId?: string,
    @Query('limit') limitRaw?: string,
    @Query('offset') offsetRaw?: string,
  ) {
    const resolvedChannelIds = channelTgChatId
      ? await this.searchService.resolveChannelIdsByTgChatIds([channelTgChatId])
      : undefined;

    return this.searchService.listTags({
      channelIds: resolvedChannelIds,
      limit: limitRaw ? Number.parseInt(limitRaw, 10) : undefined,
      offset: offsetRaw ? Number.parseInt(offsetRaw, 10) : undefined,
      role: 'admin',
    });
  }

  /**
   * GET /api/search/internal/tags/level2?level1Id=1&channelTgChatId=-100xxx&limit=30&offset=0
   * 机器人内部二级分类面板接口（X-Internal-Token 鉴权）
   */
  @UseGuards(InternalTokenGuard)
  @Get('internal/tags/level2')
  async internalLevel2Tags(
    @Query('level1Id') level1IdRaw?: string,
    @Query('channelTgChatId') channelTgChatId?: string,
    @Query('limit') limitRaw?: string,
    @Query('offset') offsetRaw?: string,
  ) {
    const resolvedChannelIds = channelTgChatId
      ? await this.searchService.resolveChannelIdsByTgChatIds([channelTgChatId])
      : undefined;

    return this.searchService.listLevel2Tags({
      level1Id: level1IdRaw ? Number.parseInt(level1IdRaw, 10) : 0,
      channelIds: resolvedChannelIds,
      limit: limitRaw ? Number.parseInt(limitRaw, 10) : undefined,
      offset: offsetRaw ? Number.parseInt(offsetRaw, 10) : undefined,
      role: 'admin',
    });
  }

  /**
   * GET /api/search/internal/by-tag?tagId=123&channelTgChatId=-100xxx&limit=20&offset=0&fallbackToDb=true
   * 机器人内部按分类搜索接口（X-Internal-Token 鉴权）
   */
  @UseGuards(InternalTokenGuard)
  @Get('internal/by-tag')
  async internalByTag(
    @Query('tagId') tagIdRaw?: string,
    @Query('tagName') tagName?: string,
    @Query('level1Id') level1IdRaw?: string,
    @Query('channelTgChatId') channelTgChatId?: string,
    @Query('limit') limitRaw?: string,
    @Query('offset') offsetRaw?: string,
    @Query('fallbackToDb') fallbackToDbRaw?: string,
  ) {
    const resolvedChannelIds = channelTgChatId
      ? await this.searchService.resolveChannelIdsByTgChatIds([channelTgChatId])
      : undefined;

    return this.searchService.searchByTag({
      tagId: tagIdRaw ? Number.parseInt(tagIdRaw, 10) : undefined,
      tagName,
      level1Id: level1IdRaw ? Number.parseInt(level1IdRaw, 10) : undefined,
      channelIds: resolvedChannelIds,
      limit: limitRaw ? Number.parseInt(limitRaw, 10) : undefined,
      offset: offsetRaw ? Number.parseInt(offsetRaw, 10) : undefined,
      fallbackToDb: fallbackToDbRaw ? fallbackToDbRaw !== 'false' : true,
      role: 'admin',
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

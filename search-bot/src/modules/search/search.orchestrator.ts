import { getJson, setJsonWithTtl } from '../../infra/redis';
import { logger } from '../../infra/logger';
import { querySearch, type SearchQueryResponse } from './search.client';

const SEARCH_CACHE_TTL_SEC = 30;

interface SearchCachePayload {
  total: number;
  hasMore: boolean;
  results: Array<Record<string, unknown>>;
  route?: 'search-engine' | 'db';
}

interface SearchArgs {
  keyword: string;
  channelId: string;
  limit: number;
  offset: number;
}

function buildCacheKey(args: SearchArgs) {
  return `sb:search:${args.channelId}:${args.keyword}:${args.limit}:${args.offset}`;
}

function fallbackResponse(): SearchQueryResponse {
  return {
    results: [],
    total: 0,
    hasMore: false,
    route: 'db',
  };
}

export async function searchWithCache(args: SearchArgs): Promise<SearchQueryResponse> {
  const key = buildCacheKey(args);
  const requestMeta = {
    keyword: args.keyword,
    channelId: args.channelId,
    limit: args.limit,
    offset: args.offset,
    cacheKey: key,
  };

  const cached = await getJson<SearchCachePayload>(key);
  if (cached) {
    logger.info('search.cache_hit', {
      ...requestMeta,
      total: cached.total,
      hasMore: cached.hasMore,
      route: cached.route,
    });
    return cached;
  }

  logger.info('search.cache_miss', requestMeta);

  try {
    const response = await querySearch({
      keyword: args.keyword,
      channelIds: [args.channelId],
      limit: args.limit,
      offset: args.offset,
      fallbackToDb: true,
    });

    logger.info('search.api_result', {
      ...requestMeta,
      total: response.total,
      hasMore: response.hasMore,
      route: response.route,
      firstItemTitle: String(response.results?.[0]?.title ?? ''),
    });

    await setJsonWithTtl(key, response, SEARCH_CACHE_TTL_SEC);
    return response;
  } catch (error) {
    logger.error('search.api_failed_fallback', {
      ...requestMeta,
      error: error instanceof Error ? error.message : String(error),
    });
    return fallbackResponse();
  }
}

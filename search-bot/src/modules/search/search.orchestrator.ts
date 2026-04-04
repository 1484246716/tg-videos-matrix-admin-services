import { getJson, setJsonWithTtl } from '../../infra/redis';
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
  const cached = await getJson<SearchCachePayload>(key);
  if (cached) {
    return cached;
  }

  try {
    const response = await querySearch({
      keyword: args.keyword,
      channelIds: [args.channelId],
      limit: args.limit,
      offset: args.offset,
      fallbackToDb: true,
    });

    await setJsonWithTtl(key, response, SEARCH_CACHE_TTL_SEC);
    return response;
  } catch {
    return fallbackResponse();
  }
}

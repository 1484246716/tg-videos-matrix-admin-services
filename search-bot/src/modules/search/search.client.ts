import axios from 'axios';
import { env } from '../../config/env';
import { logger } from '../../infra/logger';

export interface SearchQueryRequest {
  keyword: string;
  channelIds: string[];
  limit?: number;
  offset?: number;
  fallbackToDb?: boolean;
}

export interface SearchQueryResponse {
  results: Array<Record<string, unknown>>;
  total: number;
  hasMore: boolean;
  route?: 'search-engine' | 'db';
}

export interface HotQueryResponse {
  results: Array<Record<string, unknown>>;
  total: number;
  hasMore: boolean;
  route?: 'search-engine' | 'db';
}

export interface TagItem {
  id: string;
  name: string;
  level: number;
  level1Name?: string;
  count: number;
}

export interface TagsQueryResponse {
  tags: TagItem[];
  total: number;
  hasMore: boolean;
  route?: 'search-engine' | 'db';
}

const http = axios.create({
  baseURL: env.API_BASE_URL,
  timeout: 800,
  headers: {
    'Content-Type': 'application/json',
    'X-Internal-Token': env.API_INTERNAL_TOKEN,
  },
});

export async function querySearch(payload: SearchQueryRequest): Promise<SearchQueryResponse> {
  let attempts = 0;
  let lastError: unknown;

  while (attempts < 2) {
    attempts += 1;
    try {
      logger.info('search.api_request', {
        attempt: attempts,
        baseURL: env.API_BASE_URL,
        endpoint: '/api/search/internal',
        keyword: payload.keyword,
        channelIds: payload.channelIds,
        limit: payload.limit,
        offset: payload.offset,
      });

      const response = await http.get<SearchQueryResponse>('/api/search/internal', {
        params: {
          keyword: payload.keyword,
          channelTgChatId: payload.channelIds[0],
          limit: payload.limit,
          offset: payload.offset,
          fallbackToDb: payload.fallbackToDb,
        },
      });

      logger.info('search.api_response', {
        attempt: attempts,
        status: response.status,
        total: response.data?.total,
        hasMore: response.data?.hasMore,
      });

      return response.data;
    } catch (error) {
      lastError = error;
      logger.error('search.api_error', {
        attempt: attempts,
        error: error instanceof Error ? error.message : String(error),
      });
      if (attempts >= 2) break;
      await wait(80 * attempts);
    }
  }

  throw lastError;
}

export async function queryHot(payload: {
  channelIds: string[];
  limit?: number;
  offset?: number;
  period?: '3d' | '7d' | '30d';
  fallbackToDb?: boolean;
}): Promise<HotQueryResponse> {
  const startedAt = Date.now();
  const response = await http.get<HotQueryResponse>('/api/search/internal/hot', {
    params: {
      channelTgChatId: payload.channelIds[0],
      limit: payload.limit,
      offset: payload.offset,
      period: payload.period || '7d',
      fallbackToDb: payload.fallbackToDb,
    },
  });

  logger.info('search.route_metric', {
    feature: 'rm',
    route: response.data?.route,
    total: response.data?.total,
    durationMs: Date.now() - startedAt,
    channelId: payload.channelIds[0],
  });

  return response.data;
}

export async function queryTags(payload: {
  channelIds: string[];
  limit?: number;
  offset?: number;
}): Promise<TagsQueryResponse> {
  const response = await http.get<TagsQueryResponse>('/api/search/internal/tags', {
    params: {
      channelTgChatId: payload.channelIds[0],
      limit: payload.limit,
      offset: payload.offset,
    },
  });
  return response.data;
}

export async function queryLevel2Tags(payload: {
  channelIds: string[];
  level1Id: string;
  limit?: number;
  offset?: number;
}): Promise<TagsQueryResponse> {
  const response = await http.get<TagsQueryResponse>('/api/search/internal/tags/level2', {
    params: {
      channelTgChatId: payload.channelIds[0],
      level1Id: payload.level1Id,
      limit: payload.limit,
      offset: payload.offset,
    },
  });
  return response.data;
}

export async function queryByTag(payload: {
  channelIds: string[];
  tagId?: string;
  tagName?: string;
  limit?: number;
  offset?: number;
  fallbackToDb?: boolean;
}): Promise<SearchQueryResponse> {
  const startedAt = Date.now();
  const response = await http.get<SearchQueryResponse>('/api/search/internal/by-tag', {
    params: {
      channelTgChatId: payload.channelIds[0],
      tagId: payload.tagId,
      tagName: payload.tagName,
      limit: payload.limit,
      offset: payload.offset,
      fallbackToDb: payload.fallbackToDb,
    },
  });

  logger.info('search.route_metric', {
    feature: 'tags',
    route: response.data?.route,
    total: response.data?.total,
    durationMs: Date.now() - startedAt,
    channelId: payload.channelIds[0],
    tagId: payload.tagId,
    tagName: payload.tagName,
  });

  return response.data;
}

function wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

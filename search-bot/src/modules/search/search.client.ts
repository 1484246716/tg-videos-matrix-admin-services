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

function wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

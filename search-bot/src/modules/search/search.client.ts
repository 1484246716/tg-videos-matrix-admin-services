import axios from 'axios';
import { env } from '../../config/env';

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
      const response = await http.post<SearchQueryResponse>('/api/search/query', payload);
      return response.data;
    } catch (error) {
      lastError = error;
      if (attempts >= 2) break;
      await wait(80 * attempts);
    }
  }

  throw lastError;
}

function wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

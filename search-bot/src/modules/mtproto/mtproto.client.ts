import axios from 'axios';
import { env } from '../../config/env';

const http = axios.create({
  baseURL: env.MTPROTO_EXECUTOR_BASE_URL,
  timeout: 5000,
  headers: {
    'Content-Type': 'application/json',
    'X-Internal-Token': env.MTPROTO_EXECUTOR_INTERNAL_TOKEN,
  },
});

export async function injectStartViaMtproto(args: {
  chatId: string;
  startPayload: string;
  deleteDelayMs?: number;
}) {
  const response = await http.post('/internal/mtproto/inject-start', {
    chatId: args.chatId,
    startPayload: args.startPayload,
    deleteDelayMs: args.deleteDelayMs,
  });

  return response.data;
}

import dotenv from 'dotenv';
import axios from 'axios';

dotenv.config();

const BASE_URL = process.env.SEARCH_BOT_WEBHOOK_BASE_URL || 'http://localhost:3310';
const SECRET = process.env.BOT_WEBHOOK_SECRET || 'dev-webhook-secret';

const endpoint = `${BASE_URL.replace(/\/$/, '')}/telegram/webhook/${SECRET}`;

async function sendMessageUpdate(updateId: number, text: string) {
  return axios.post(endpoint, {
    update_id: updateId,
    message: {
      message_id: 1000 + updateId,
      date: Math.floor(Date.now() / 1000),
      chat: { id: -1001234567890, type: 'supergroup' },
      from: { id: 9527 },
      text,
    },
  });
}

async function sendCallbackUpdate(updateId: number, token: string) {
  return axios.post(endpoint, {
    update_id: updateId,
    callback_query: {
      id: `cbq-${updateId}`,
      from: { id: 9527 },
      data: `sp:${token}`,
      message: {
        message_id: 8888,
        chat: { id: -1001234567890 },
      },
    },
  });
}

async function main() {
  // 1) 指令冒烟
  const msgRes = await sendMessageUpdate(9100001, '/s 沈腾');
  // eslint-disable-next-line no-console
  console.log('[smoke] message route =>', msgRes.data);

  // 2) 幂等冒烟（同 update_id 再发一次）
  const idemRes = await sendMessageUpdate(9100001, '/s 沈腾');
  // eslint-disable-next-line no-console
  console.log('[smoke] idempotent =>', idemRes.data);

  // 3) callback 过期分支冒烟（随机token通常不存在）
  const cbRes = await sendCallbackUpdate(9100002, 'not-exist-token');
  // eslint-disable-next-line no-console
  console.log('[smoke] callback route =>', cbRes.data);
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('[smoke] failed:', err?.response?.data || err.message || err);
  process.exit(1);
});

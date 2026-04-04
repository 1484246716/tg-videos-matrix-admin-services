import dotenv from 'dotenv';
import axios from 'axios';

dotenv.config();

const BOT_TOKEN = process.env.BOT_TOKEN;
const WEBHOOK_BASE_URL = process.env.SEARCH_BOT_WEBHOOK_BASE_URL;
const WEBHOOK_SECRET = process.env.BOT_WEBHOOK_SECRET;

if (!BOT_TOKEN || !WEBHOOK_BASE_URL || !WEBHOOK_SECRET) {
  // eslint-disable-next-line no-console
  console.error('缺少必要环境变量：BOT_TOKEN / SEARCH_BOT_WEBHOOK_BASE_URL / BOT_WEBHOOK_SECRET');
  process.exit(1);
}

const webhookUrl = `${WEBHOOK_BASE_URL.replace(/\/$/, '')}/telegram/webhook/${WEBHOOK_SECRET}`;

async function main() {
  const endpoint = `https://api.telegram.org/bot${BOT_TOKEN}/setWebhook`;

  const response = await axios.post(endpoint, {
    url: webhookUrl,
    drop_pending_updates: false,
  });

  // eslint-disable-next-line no-console
  console.log('[set-webhook] done:', response.data);
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('[set-webhook] failed:', err?.response?.data || err.message || err);
  process.exit(1);
});

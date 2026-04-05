import { Request, Response } from 'express';
import { env } from '../../config/env';
import { injectStartCommand } from './executor.service';

interface InjectStartBody {
  chatId?: string;
  startPayload?: string;
  deleteDelayMs?: number;
}

export async function postInjectStart(req: Request, res: Response) {
  const token = req.headers['x-internal-token'];
  if (token !== env.MTPROTO_EXECUTOR_INTERNAL_TOKEN) {
    return res.status(403).json({ ok: false, message: 'forbidden' });
  }

  const body = req.body as InjectStartBody;
  if (!body.chatId || !body.startPayload) {
    return res.status(400).json({ ok: false, message: 'chatId/startPayload required' });
  }

  try {
    const result = await injectStartCommand({
      chatId: body.chatId,
      startPayload: body.startPayload,
      deleteDelayMs: body.deleteDelayMs,
    });

    return res.status(200).json({ ok: true, result });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error instanceof Error ? error.message : String(error),
    });
  }
}

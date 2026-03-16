import { basename } from 'node:path';
import { getGramjsClient } from './client';
import type { GramjsSendResult } from './types';

export async function sendViaGramjs(args: {
  filePath: string;
  fileName?: string;
  caption?: string;
  chatId: string;
  workers?: number;
  progressCallback?: (progress: number) => void;
}): Promise<GramjsSendResult> {
  const client = await getGramjsClient();
  const fileName = args.fileName ?? basename(args.filePath);

  const message = await client.sendFile(args.chatId, {
    file: args.filePath,
    caption: args.caption ?? fileName,
    fileName,
    forceDocument: true,
    workers: args.workers,
    progressCallback: args.progressCallback,
  });

  if (!message?.id) {
    throw new Error('GramJS 上传成功但未返回 message_id');
  }

  return {
    messageId: Number(message.id),
  };
}

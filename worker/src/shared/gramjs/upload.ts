import { basename } from 'node:path';
import { Api } from 'telegram';
import { getGramjsClient } from './client';
import type { GramjsSendResult, GramjsVideoMeta } from './types';

export async function sendViaGramjs(args: {
  filePath: string;
  fileName?: string;
  caption?: string;
  chatId: string;
  forceDocument?: boolean;
  workers?: number;
  progressCallback?: (progress: number) => void;
  videoMeta?: GramjsVideoMeta;
}): Promise<GramjsSendResult> {
  const client = await getGramjsClient();
  const fileName = args.fileName ?? basename(args.filePath);

  const durationSec = Math.max(0, Math.floor(args.videoMeta?.durationSec ?? 0));
  const width = Math.max(0, Math.floor(args.videoMeta?.width ?? 0));
  const height = Math.max(0, Math.floor(args.videoMeta?.height ?? 0));

  const attributes = [
    new Api.DocumentAttributeFilename({
      fileName,
    }),
    new Api.DocumentAttributeVideo({
      roundMessage: false,
      supportsStreaming: args.videoMeta?.supportsStreaming ?? true,
      nosound: false,
      duration: durationSec,
      w: width,
      h: height,
    }),
  ];

  const message = await client.sendFile(args.chatId, {
    file: args.filePath,
    caption: args.caption ?? fileName,
    forceDocument: args.forceDocument ?? false,
    workers: args.workers,
    progressCallback: args.progressCallback,
    supportsStreaming: args.videoMeta?.supportsStreaming ?? true,
    attributes,
  } as any);

  if (!message?.id) {
    throw new Error('GramJS 上传成功但未返回 message_id');
  }

  return {
    messageId: Number(message.id),
  };
}

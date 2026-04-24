/**
 * GramJS 上传封装：提供 Telegram 文件发送能力。
 * 为 relay / clone-channels 共享大文件上传与消息发送实现。
 */

import { basename } from 'node:path';
import { stat } from 'node:fs/promises';
import { Api } from 'telegram';
import { getGramjsClient } from './client';
import type { GramjsSendResult, GramjsVideoMeta } from './types';

// 通过 GramJS 发送媒体文件并返回消息标识。
export async function sendViaGramjs(args: {
  filePath: string;
  fileName?: string;
  caption?: string;
  chatId: string;
  forceDocument?: boolean;
  workers?: number;
  progressCallback?: (progress: number) => void;
  videoMeta?: GramjsVideoMeta;
  thumbnailPath?: string;
}): Promise<GramjsSendResult> {
  const client = await getGramjsClient();
  const fileName = args.fileName ?? basename(args.filePath);

  // 获取文件大小
  const fileStat = await stat(args.filePath);
  const fileSize = Number(fileStat.size);

  const durationSecRaw = args.videoMeta?.durationSec;
  const widthRaw = args.videoMeta?.width;
  const heightRaw = args.videoMeta?.height;

  const durationSec =
    typeof durationSecRaw === 'number' && Number.isFinite(durationSecRaw) && durationSecRaw > 0
      ? Math.floor(durationSecRaw)
      : null;
  const width =
    typeof widthRaw === 'number' && Number.isFinite(widthRaw) && widthRaw > 0
      ? Math.floor(widthRaw)
      : null;
  const height =
    typeof heightRaw === 'number' && Number.isFinite(heightRaw) && heightRaw > 0
      ? Math.floor(heightRaw)
      : null;

  // Telegram MTProto 要求 duration 必须为合法非负整数（int），不能缺失也不能为 double
  // w/h 若为 0 或缺失则不传，避免触发 DOUBLE_VALUE_INVALID
  // size 必须传递，Telegram 用它在客户端显示文件大小
  const videoAttrData: Record<string, unknown> = {
    roundMessage: false,
    supportsStreaming: args.videoMeta?.supportsStreaming ?? true,
    nosound: false,
    duration: durationSec !== null ? durationSec : 0,
    size: fileSize,
  };
  if (width !== null && width > 0) videoAttrData.w = width;
  if (height !== null && height > 0) videoAttrData.h = height;

  const attributes = [
    new Api.DocumentAttributeFilename({ fileName }),
    new Api.DocumentAttributeVideo(videoAttrData as any),
  ];

  const message = await client.sendFile(args.chatId, {
    file: args.filePath,
    caption: args.caption ?? fileName,
    forceDocument: args.forceDocument ?? false,
    workers: args.workers,
    progressCallback: args.progressCallback,
    supportsStreaming: args.videoMeta?.supportsStreaming ?? true,
    attributes,
    thumb: args.thumbnailPath,
  } as any);

  if (!message?.id) {
    throw new Error('GramJS 上传成功但未返回 message_id');
  }

  return {
    messageId: Number(message.id),
  };
}

/**
 * GramJS 类型定义：描述 Telegram 上传/发送所需数据结构。
 * 为 relay / clone-channels 共享 GramJS 相关入参与返回值类型。
 */

export type GramjsVideoMeta = {
  durationSec?: number | null;
  width?: number | null;
  height?: number | null;
  supportsStreaming?: boolean;
};

export type GramjsSendResult = {
  messageId: number;
};

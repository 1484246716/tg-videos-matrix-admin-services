export type GramjsVideoMeta = {
  durationSec?: number | null;
  width?: number | null;
  height?: number | null;
  supportsStreaming?: boolean;
};

export type GramjsSendResult = {
  messageId: number;
};

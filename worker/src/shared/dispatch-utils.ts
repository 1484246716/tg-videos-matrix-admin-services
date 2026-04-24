/**
 * 分发链路通用工具：供 scheduler/worker/service 共享使用。
 * 目前提供重试退避时间计算等轻量工具函数。
 */

// 根据重试次数计算退避秒数（指数退避并封顶）。
export function getBackoffSeconds(retryCount: number): number {
  const base = Math.max(1, Math.pow(2, retryCount));
  return Math.min(base * 30, 3600);
}

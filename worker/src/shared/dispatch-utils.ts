export function getBackoffSeconds(retryCount: number): number {
  const base = Math.max(1, Math.pow(2, retryCount));
  return Math.min(base * 30, 3600);
}

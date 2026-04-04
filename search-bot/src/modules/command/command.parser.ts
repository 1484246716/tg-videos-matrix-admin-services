export interface ParsedSearchCommand {
  keyword: string;
}

export function parseSearchCommand(text: string | undefined | null): ParsedSearchCommand | null {
  if (!text) return null;
  const normalized = text.trim();
  if (!normalized) return null;

  if (!normalized.startsWith('/s')) return null;

  const keyword = normalized.slice(2).trim();
  if (keyword.length < 2) return null;

  return { keyword };
}

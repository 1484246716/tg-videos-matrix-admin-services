export interface ParsedSearchCommand {
  keyword: string;
}

export type ParsedCommand =
  | { type: 'search'; keyword: string }
  | { type: 'rm' }
  | { type: 'tags' }
  | { type: 'unknown' };

export function parseCommand(text: string | undefined | null): ParsedCommand {
  const normalized = String(text || '').trim();
  if (!normalized) return { type: 'unknown' };

  if (normalized === '/rm') return { type: 'rm' };
  if (normalized === '/tags') return { type: 'tags' };

  if (normalized.startsWith('/s')) {
    const keyword = normalized.slice(2).trim();
    if (keyword.length >= 2) return { type: 'search', keyword };
  }

  return { type: 'unknown' };
}

export function parseSearchCommand(text: string | undefined | null): ParsedSearchCommand | null {
  const parsed = parseCommand(text);
  if (parsed.type !== 'search') return null;
  return { keyword: parsed.keyword };
}

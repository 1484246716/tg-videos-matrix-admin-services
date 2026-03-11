import { createReadStream } from 'node:fs';
import { mkdir, readdir, rename, stat } from 'node:fs/promises';
import { basename, dirname, extname, join, resolve } from 'node:path';
import { createHash } from 'node:crypto';
import {
  RELAY_MIN_STABLE_CHECKS,
  RELAY_MTIME_COOLDOWN_MS,
  RELAY_STABLE_INTERVAL_MS,
} from '../config/env';
import { logger } from '../logger';

const SUPPORTED_VIDEO_EXT = new Set([
  '.mp4',
  '.mkv',
  '.mov',
  '.avi',
  '.m4v',
  '.webm',
]);

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export async function moveToArchive(localPath: string): Promise<string> {
  if (localPath.includes(`${join('archived')}`)) {
    return localPath;
  }

  const fileName = basename(localPath);
  const channelIdDir = dirname(localPath);
  const channelDirName = basename(channelIdDir);
  const channelsDir = dirname(channelIdDir);
  const dataBaseDir = dirname(channelsDir);
  const archiveDir = join(dataBaseDir, 'archived', channelDirName);
  await mkdir(archiveDir, { recursive: true });
  const archivePath = join(archiveDir, fileName);
  await rename(localPath, archivePath);
  logger.info('[archive] moved file', { from: localPath, to: archivePath });
  return archivePath;
}

export async function waitForFileStable(filePath: string) {
  let previousSize: number | null = null;
  let stableCount = 0;

  while (stableCount < RELAY_MIN_STABLE_CHECKS) {
    const s = await stat(filePath);

    if (previousSize !== null && Number(s.size) === previousSize) {
      stableCount += 1;
    } else {
      stableCount = 0;
      previousSize = Number(s.size);
    }

    if (stableCount < RELAY_MIN_STABLE_CHECKS) {
      await sleep(RELAY_STABLE_INTERVAL_MS);
    }
  }

  const finalStat = await stat(filePath);
  const ageMs = Date.now() - finalStat.mtimeMs;
  if (ageMs < RELAY_MTIME_COOLDOWN_MS) {
    await sleep(RELAY_MTIME_COOLDOWN_MS - ageMs);
  }
}

export async function hashFile(filePath: string): Promise<string> {
  return new Promise((resolveHash, reject) => {
    const hash = createHash('sha256');
    const stream = createReadStream(filePath);

    stream.on('data', (chunk) => hash.update(chunk));
    stream.on('end', () => resolveHash(hash.digest('hex')));
    stream.on('error', reject);
  });
}

export async function scanChannelVideos(folderPath: string) {
  const absolute = resolve(folderPath);
  const entries = await readdir(absolute, { withFileTypes: true });

  const files = entries
    .filter((entry) => entry.isFile())
    .map((entry) => resolve(absolute, entry.name))
    .filter((filePath) => SUPPORTED_VIDEO_EXT.has(extname(filePath).toLowerCase()));

  return files;
}

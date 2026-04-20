import { createReadStream } from 'node:fs';
import { mkdir, readdir, rename, stat, unlink, open } from 'node:fs/promises';
import { basename, dirname, extname, join, normalize, resolve } from 'node:path';
import { createHash } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import {
  RELAY_ENABLE_FFPROBE_CHECK,
  RELAY_FFPROBE_MIN_DURATION_SEC,
  RELAY_FFPROBE_TIMEOUT_MS,
  RELAY_MIN_STABLE_CHECKS,
  RELAY_MTIME_COOLDOWN_MS,
  RELAY_STABLE_INTERVAL_MS,
  TYPEA_GROUP_SCAN_ENABLED,
  TYPEA_GROUP_SCAN_MAX_DIRS_PER_TICK,
  TYPEA_GROUP_SCAN_MAX_FILES_PER_TICK,
  TYPEA_GROUP_SCAN_CONCURRENCY,
} from '../config/env';
import { logger } from '../logger';

const SUPPORTED_VIDEO_EXT = new Set([
  '.mp4',
  '.mkv',
  '.mov',
  '.avi',
  '.webm',
  '.flv',
  '.wmv',
  '.mpeg',
  '.mpg',
  '.3gp',
  '.m4v',
]);

const SUPPORTED_IMAGE_EXT = new Set(['.jpg', '.jpeg', '.png', '.webp', '.gif']);
const SUPPORTED_TEXT_EXT = new Set(['.txt']);


const execFileAsync = promisify(execFile);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export type VideoProbeMeta = {
  durationSec: number | null;
  width: number | null;
  height: number | null;
  supportsStreaming: boolean;
};

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
  logger.info('[archive] 文件已归档', { from: localPath, to: archivePath });
  return archivePath;
}

async function probeVideoByFfprobe(filePath: string) {
  const probeStart = Date.now();
  logger.info('[relay] ffprobe 开始探测', {
    stage: 'ffprobe_start',
    filePath,
    timeoutMs: RELAY_FFPROBE_TIMEOUT_MS,
    minDurationSec: RELAY_FFPROBE_MIN_DURATION_SEC,
  });

  const { stdout } = await execFileAsync(
    'ffprobe',
    [
      '-v',
      'error',
      '-show_streams',
      '-show_format',
      '-of',
      'json',
      filePath,
    ],
    { timeout: RELAY_FFPROBE_TIMEOUT_MS },
  );

  let parsed: any;
  try {
    parsed = JSON.parse(stdout || '{}');
  } catch {
    throw new Error('ffprobe 输出非 JSON，无法解析');
  }

  const streams = Array.isArray(parsed?.streams) ? parsed.streams : [];
  const hasVideoStream = streams.some((s: any) => s?.codec_type === 'video');
  if (!hasVideoStream) {
    throw new Error('ffprobe 未检测到视频流');
  }

  const durationRaw = parsed?.format?.duration;
  const duration = Number(durationRaw);
  if (!Number.isFinite(duration) || duration < RELAY_FFPROBE_MIN_DURATION_SEC) {
    throw new Error(
      `ffprobe 时长异常: duration=${String(durationRaw)}, min=${RELAY_FFPROBE_MIN_DURATION_SEC}`,
    );
  }

  logger.info('[relay] ffprobe 探测通过', {
    stage: 'ffprobe_pass',
    filePath,
    durationSec: duration,
    streamCount: streams.length,
    elapsedMs: Date.now() - probeStart,
  });
}

export async function getVideoProbeMeta(filePath: string): Promise<VideoProbeMeta> {
  const { stdout } = await execFileAsync(
    'ffprobe',
    [
      '-v',
      'error',
      '-show_streams',
      '-show_format',
      '-of',
      'json',
      filePath,
    ],
    { timeout: RELAY_FFPROBE_TIMEOUT_MS },
  );

  let parsed: any;
  try {
    parsed = JSON.parse(stdout || '{}');
  } catch {
    throw new Error('ffprobe 输出非 JSON，无法解析');
  }

  const streams = Array.isArray(parsed?.streams) ? parsed.streams : [];
  const videoStream = streams.find((s: any) => s?.codec_type === 'video') ?? null;
  if (!videoStream) {
    throw new Error('ffprobe 未检测到视频流');
  }

  const durationRaw = videoStream?.duration ?? parsed?.format?.duration;
  const durationNum = Number(durationRaw);
  const widthNum = Number(videoStream?.width);
  const heightNum = Number(videoStream?.height);
  const formatName = String(parsed?.format?.format_name || '').toLowerCase();
  const majorBrand = String(parsed?.format?.tags?.major_brand || '').toLowerCase();
  const compatibleBrands = String(parsed?.format?.tags?.compatible_brands || '').toLowerCase();
  const codecName = String(videoStream?.codec_name || '').toLowerCase();

  return {
    durationSec: Number.isFinite(durationNum) && durationNum > 0 ? Math.floor(durationNum) : null,
    width: Number.isFinite(widthNum) && widthNum > 0 ? widthNum : null,
    height: Number.isFinite(heightNum) && heightNum > 0 ? heightNum : null,
    supportsStreaming:
      formatName.includes('mov,mp4') ||
      majorBrand.includes('mp4') ||
      compatibleBrands.includes('mp4') ||
      codecName === 'h264' ||
      codecName === 'hevc',
  };
}

export async function ensureMp4Faststart(filePath: string): Promise<string> {
  const ext = extname(filePath).toLowerCase();
  if (ext !== '.mp4') {
    return filePath;
  }

  const tempPath = `${filePath}.faststart.tmp.mp4`;

  try {
    await execFileAsync(
      'ffmpeg',
      [
        '-y',
        '-i',
        filePath,
        '-c',
        'copy',
        '-movflags',
        '+faststart',
        tempPath,
      ],
      { timeout: Math.max(RELAY_FFPROBE_TIMEOUT_MS * 4, 60000) },
    );

    await unlink(filePath);
    await rename(tempPath, filePath);
    logger.info('[relay] MP4 faststart 预处理完成', {
      stage: 'faststart_done',
      filePath,
    });
    return filePath;
  } catch (error) {
    try {
      await unlink(tempPath);
    } catch {
      // ignore cleanup failure
    }
    throw error instanceof Error ? error : new Error(String(error));
  }
}

export async function createVideoThumbnail(filePath: string): Promise<string> {
  const thumbnailPath = `${filePath}.tg-thumb.jpg`;

  await execFileAsync(
    'ffmpeg',
    [
      '-y',
      '-ss',
      '1',
      '-i',
      filePath,
      '-frames:v',
      '1',
      '-vf',
      'scale=320:-1',
      '-q:v',
      '3',
      thumbnailPath,
    ],
    { timeout: Math.max(RELAY_FFPROBE_TIMEOUT_MS * 3, 45000) },
  );

  return thumbnailPath;
}

export async function waitForFileStable(filePath: string) {
  const stableStart = Date.now();
  logger.info('[relay] 文件稳定性检查开始', {
    stage: 'stable_check_start',
    filePath,
    minStableChecks: RELAY_MIN_STABLE_CHECKS,
    stableIntervalMs: RELAY_STABLE_INTERVAL_MS,
    mtimeCooldownMs: RELAY_MTIME_COOLDOWN_MS,
    ffprobeEnabled: RELAY_ENABLE_FFPROBE_CHECK,
  });

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

  logger.info('[relay] 静默期判定结果', {
    stage: 'mtime_cooldown_check',
    filePath,
    fileSize: Number(finalStat.size),
    ageMs: Math.floor(ageMs),
    requiredCooldownMs: RELAY_MTIME_COOLDOWN_MS,
    passed: ageMs >= RELAY_MTIME_COOLDOWN_MS,
  });

  if (ageMs < RELAY_MTIME_COOLDOWN_MS) {
    throw new Error(
      `文件处于静默冷却期，最后修改距今 ${Math.floor(ageMs / 1000)} 秒，要求至少 ${Math.floor(RELAY_MTIME_COOLDOWN_MS / 1000)} 秒`,
    );
  }

  if (!RELAY_ENABLE_FFPROBE_CHECK) {
    logger.info('[relay] 已跳过 ffprobe 探测（配置关闭）', {
      stage: 'ffprobe_skipped',
      filePath,
    });
    logger.info('[relay] 文件稳定性检查通过', {
      stage: 'stable_check_pass',
      filePath,
      elapsedMs: Date.now() - stableStart,
      ffprobeEnabled: false,
    });
    return;
  }

  try {
    await probeVideoByFfprobe(filePath);
  } catch (error) {
    const reason = error instanceof Error ? error.message : String(error);
    logger.warn('[relay] ffprobe 检测未通过，跳过', {
      stage: 'ffprobe_fail',
      filePath,
      reason,
      elapsedMs: Date.now() - stableStart,
    });
    throw new Error(`ffprobe 检测未通过: ${reason}`);
  }

  logger.info('[relay] 文件稳定性检查通过', {
    stage: 'stable_check_pass',
    filePath,
    elapsedMs: Date.now() - stableStart,
    ffprobeEnabled: true,
  });
}

/**
 * 局部采样快速哈希算法 (用于大文件秒传去重)
 * 相比于全量读取几十上百兆的视频做 Hash (会导致 CPU 打满并阻塞长达几十分钟)，
 * 我们通过混合文件大小，并只抽样读取视频的头部、中部和尾部各 1MB 进行 Hash。
 * 由于只有在视频文件写入完成后才会调用本函数，所以计算结果稳定可靠，且耗时降至几十毫秒级别。
 */
export async function hashFile(filePath: string): Promise<string> {
  const CHUNK_SIZE = 1024 * 1024; // 抽取 1MB 块
  const handle = await open(filePath, 'r');
  
  try {
    const fileStat = await handle.stat();
    const fileSize = fileStat.size;
    const hash = createHash('sha256');
    
    // 1. 混合文件大小作为盐值，避免同 Hash 冲突
    hash.update(fileSize.toString());

    if (fileSize <= CHUNK_SIZE * 3) {
      // 如果文件很小 (比如小于 3MB)，直接全量读
      const buf = await handle.readFile();
      hash.update(buf);
    } else {
      // 对于大文件，抽取：头部1M，中部1M，尾部1M
      const chunks = [
        { start: 0, length: CHUNK_SIZE },
        { start: Math.floor(fileSize / 2), length: CHUNK_SIZE },
        { start: fileSize - CHUNK_SIZE, length: CHUNK_SIZE }
      ];

      for (const { start, length } of chunks) {
        const buf = Buffer.alloc(length);
        await handle.read(buf, 0, length, start);
        hash.update(buf);
      }
    }
    return hash.digest('hex');
  } finally {
    await handle.close();
  }
}

export function normalizeRelayPath(filePath: string): string {
  let normalized = filePath.trim().replace(/\\/g, '/');
  normalized = normalized.replace(/\/+/g, '/');

  if (process.platform === 'win32') {
    normalized = normalized.toLowerCase();
  }

  if (normalized.length > 1) {
    normalized = normalized.replace(/\/+$/, '');
  }

  return normalized;
}

export function buildRelayPathFingerprint(channelId: bigint, filePath: string): {
  pathNormalized: string;
  pathFingerprint: string;
} {
  const pathNormalized = normalizeRelayPath(filePath);
  const pathFingerprint = createHash('sha256')
    .update(`${channelId.toString()}|${pathNormalized}`)
    .digest('hex');

  return { pathNormalized, pathFingerprint };
}

export async function scanChannelVideos(folderPath: string) {
  const rawRoot = (process.env.CHANNELS_ROOT_DIR || './data/channels').trim();

  const isWindowsDrivePath = process.platform === 'win32' && /^\/[a-zA-Z]/.test(rawRoot);
  const root = isWindowsDrivePath
    ? resolve(process.cwd(), '..', rawRoot.replace(/^\//, ''))
    : resolve(rawRoot);

  const normalizedInput = normalize(folderPath.trim().replace(/\\/g, '/'));
  const relativePath = normalizedInput.replace(/^[\\/]+/, '');
  const absolute = resolve(root, relativePath);

  let entries: Array<import('node:fs').Dirent> = [];
  try {
    entries = await readdir(absolute, { withFileTypes: true, encoding: 'utf8' });
  } catch (error) {
    logger.warn('[scan] 目录不存在或不可访问', {
      folderPath,
      absolute,
      root,
      rawRoot,
      error: error instanceof Error ? error.message : String(error),
    });
    return [];
  }

  const groupedDirPattern = /^(grouped-\d+|single-\d+)$/i;
  const groupedRoots = entries
    .filter((entry) => entry.isDirectory() && groupedDirPattern.test(entry.name))
    .map((entry) => resolve(absolute, entry.name));

  const collectionDir = entries.find(
    (entry) => entry.isDirectory() && entry.name.toLowerCase() === 'collection',
  );

  const collectionRoots: string[] = [];
  if (collectionDir) {
    const collectionRoot = resolve(absolute, collectionDir.name);
    try {
      const collectionEntries = await readdir(collectionRoot, { withFileTypes: true, encoding: 'utf8' });
      for (const dirent of collectionEntries) {
        if (!dirent.isDirectory()) continue;
        const albumDir = resolve(collectionRoot, dirent.name);
        try {
          const albumEntries = await readdir(albumDir, { withFileTypes: true, encoding: 'utf8' });
          for (const node of albumEntries) {
            if (node.isDirectory() && groupedDirPattern.test(node.name)) {
              collectionRoots.push(resolve(albumDir, node.name));
            }
          }
        } catch (error) {
          logger.warn('[scan] 合集子目录不可访问，已跳过', {
            folderPath,
            albumDir,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }
    } catch (error) {
      logger.warn('[scan] Collection 目录不可访问，跳过合集扫描', {
        folderPath,
        collectionRoot,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  const allGroupDirs = [...groupedRoots, ...collectionRoots];
  const discoveredFiles: string[] = [];

  if (TYPEA_GROUP_SCAN_ENABLED && allGroupDirs.length > 0) {
    const limitedDirs = allGroupDirs.slice(0, TYPEA_GROUP_SCAN_MAX_DIRS_PER_TICK);
    let cursor = 0;
    const workerCount = Math.min(TYPEA_GROUP_SCAN_CONCURRENCY, Math.max(1, limitedDirs.length));

    const scanOneDir = async (groupDir: string) => {
      if (discoveredFiles.length >= TYPEA_GROUP_SCAN_MAX_FILES_PER_TICK) return;
      try {
        const files = await readdir(groupDir, { withFileTypes: true, encoding: 'utf8' });
        for (const item of files) {
          if (discoveredFiles.length >= TYPEA_GROUP_SCAN_MAX_FILES_PER_TICK) break;
          if (!item.isFile()) continue;
          const ext = extname(item.name).toLowerCase();
          const supported =
            SUPPORTED_VIDEO_EXT.has(ext) ||
            SUPPORTED_IMAGE_EXT.has(ext);
          if (!supported) continue;
          discoveredFiles.push(resolve(groupDir, item.name));
        }
      } catch (error) {
        logger.warn('[scan] grouped/single 目录不可访问，已跳过', {
          groupDir,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    };

    const runners = Array.from({ length: workerCount }).map(async () => {
      while (cursor < limitedDirs.length && discoveredFiles.length < TYPEA_GROUP_SCAN_MAX_FILES_PER_TICK) {
        const idx = cursor;
        cursor += 1;
        const dir = limitedDirs[idx];
        if (!dir) continue;
        await scanOneDir(dir);
      }
    });

    await Promise.all(runners);

    if (discoveredFiles.length > 0) {
      logger.info('[scan] grouped/single 文件扫描完成', {
        folderPath,
        groupedDirCount: limitedDirs.length,
        foundFiles: discoveredFiles.length,
        maxDirsPerTick: TYPEA_GROUP_SCAN_MAX_DIRS_PER_TICK,
        maxFilesPerTick: TYPEA_GROUP_SCAN_MAX_FILES_PER_TICK,
      });
      return discoveredFiles;
    }
  }

  // fallback: 保持历史扫描行为，避免影响现有 TypeA
  const rootFiles = entries
    .filter((entry) => entry.isFile())
    .map((entry) => resolve(absolute, entry.name))
    .filter((filePath) => SUPPORTED_VIDEO_EXT.has(extname(filePath).toLowerCase()));

  const collectionFiles: string[] = [];
  if (collectionDir) {
    const collectionRoot = resolve(absolute, collectionDir.name);
    let collectionEntries: Array<import('node:fs').Dirent> = [];
    try {
      collectionEntries = await readdir(collectionRoot, { withFileTypes: true, encoding: 'utf8' });
    } catch (error) {
      logger.warn('[scan] Collection 目录不可访问，跳过合集扫描', {
        folderPath,
        collectionRoot,
        error: error instanceof Error ? error.message : String(error),
      });
    }

    for (const dirent of collectionEntries) {
      if (!dirent.isDirectory()) continue;
      const albumDir = resolve(collectionRoot, dirent.name);
      try {
        const episodeEntries = await readdir(albumDir, { withFileTypes: true, encoding: 'utf8' });
        const files = episodeEntries
          .filter((item) => item.isFile())
          .map((item) => resolve(albumDir, item.name))
          .filter((filePath) => SUPPORTED_VIDEO_EXT.has(extname(filePath).toLowerCase()));
        collectionFiles.push(...files);
      } catch (error) {
        logger.warn('[scan] 合集子目录不可访问，已跳过', {
          folderPath,
          albumDir,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }
  }

  return [...rootFiles, ...collectionFiles];
}

import { createReadStream } from 'node:fs';
import { mkdir, readdir, rename, stat, unlink } from 'node:fs/promises';
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

  const files = entries
    .filter((entry) => entry.isFile())
    .map((entry) => resolve(absolute, entry.name))
    .filter((filePath) => SUPPORTED_VIDEO_EXT.has(extname(filePath).toLowerCase()));

  return files;
}

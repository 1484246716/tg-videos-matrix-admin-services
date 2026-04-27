/**
 * 测试脚本：验证 resolveChannelAbsolutePath 在所有场景下的输出
 * 用法：npx tsx src/scripts/test-path-resolve.ts
 */
import path from 'node:path';

process.env.CHANNELS_ROOT_DIR = '/data/channels';

function resolveChannelAbsolutePath_LINUX(dbTargetPath: string): string {
  if (!dbTargetPath) return process.cwd();

  const channelsRoot = (process.env.CHANNELS_ROOT_DIR || '/data/channels').trim();
  const normalized = dbTargetPath.replace(/\\/g, '/');

  // 已经在 channelsRoot 下
  if (normalized.startsWith(channelsRoot)) {
    console.log(`  ✅ 已在 channelsRoot 下 → ${normalized}`);
    return normalized;
  }

  // 策略 1: /channels/ 截取
  const channelsMatch = normalized.match(/\/channels\/(.+)$/i);
  if (channelsMatch && channelsMatch[1]) {
    const result = path.posix.resolve(channelsRoot, channelsMatch[1]);
    console.log(`  ✅ 策略1(channels截取) → ${result}`);
    return result;
  }

  // 策略 2: Windows 盘符
  const winDriveMatch = normalized.match(/^[a-zA-Z]:\//);
  if (winDriveMatch) {
    const lastSegment = normalized.split('/').filter(Boolean).pop();
    if (lastSegment) {
      const result = path.posix.resolve(channelsRoot, lastSegment);
      console.log(`  ✅ 策略2(盘符剥离) → ${result}`);
      return result;
    }
  }

  // 策略 3: 去前导斜杠，拼接到 channelsRoot
  const relativePath = normalized.replace(/^\/+/, '');
  const result = path.posix.resolve(channelsRoot, relativePath);
  console.log(`  ✅ 策略3(去前导/拼接) → ${result}`);
  return result;
}

console.log('=== resolveChannelAbsolutePath 路径转换测试 ===');
console.log(`CHANNELS_ROOT_DIR = ${process.env.CHANNELS_ROOT_DIR}\n`);

const testCases = [
  // 场景1: 数据库里存的实际值 — 带前导 /
  '/频道测试H片(-1003981411071)',
  // 场景2: 不带前导 /
  '频道测试H片(-1003981411071)',
  // 场景3: Windows 完整路径，包含 channels
  'D:\\Project\\tg-videos-matrix\\data\\tg-crm\\channels\\频道测试H片(-1003981411071)',
  // 场景4: 已经是正确的 Linux 路径
  '/data/channels/频道测试H片(-1003981411071)',
  // 场景5: Windows 路径不含 channels
  'D:\\Project\\tg-videos-matrix\\data\\tg-crm\\some-other\\频道测试H片(-1003981411071)',
];

for (const testPath of testCases) {
  console.log(`输入: "${testPath}"`);
  const result = resolveChannelAbsolutePath_LINUX(testPath);
  const groupedPath = path.posix.join(result, 'single-16966');
  console.log(`  最终 mkdir 路径: ${groupedPath}`);
  const expected = '/data/channels/频道测试H片(-1003981411071)/single-16966';
  console.log(`  ${groupedPath === expected ? '✅ 正确!' : `❌ 错误! 期望: ${expected}`}`);
  console.log('');
}

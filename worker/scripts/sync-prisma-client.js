/**
 * sync-prisma-client.js
 *
 * 在 pnpm workspace 下，prisma generate 输出到 api 的 node_modules，
 * 但 worker 可能通过 pnpm hoisting 解析到不同的 @prisma/client 副本。
 * 此脚本检测差异并自动同步 generated client 文件。
 */
const fs = require('fs');
const path = require('path');

// Worker 实际使用的 @prisma/client 路径
const workerResolvedPath = require.resolve('@prisma/client');
const workerClientDir = path.join(path.dirname(workerResolvedPath), '..', '.prisma', 'client');

// prisma generate 的输出路径（api 的 node_modules）
const apiSchemaDir = path.resolve(__dirname, '../../api/prisma');
// 查找 api 侧的 generated client
const apiNodeModules = path.resolve(__dirname, '../../api/node_modules');

function findGeneratedClient(baseDir) {
  const prismaDir = path.join(baseDir, '.prisma', 'client');
  if (fs.existsSync(prismaDir)) return prismaDir;

  // 搜索 .pnpm 下的 @prisma/client
  const pnpmDir = path.join(baseDir, '.pnpm');
  if (!fs.existsSync(pnpmDir)) return null;

  const entries = fs.readdirSync(pnpmDir).filter(e => e.startsWith('@prisma+client'));
  for (const entry of entries) {
    const candidate = path.join(pnpmDir, entry, 'node_modules', '.prisma', 'client');
    if (fs.existsSync(candidate)) return candidate;
  }
  return null;
}

const apiGeneratedDir = findGeneratedClient(apiNodeModules);

if (!apiGeneratedDir) {
  console.log('[sync-prisma] API generated client not found, skipping sync');
  process.exit(0);
}

const normalizedWorker = path.resolve(workerClientDir);
const normalizedApi = path.resolve(apiGeneratedDir);

if (normalizedWorker === normalizedApi) {
  console.log('[sync-prisma] Worker and API use the same client path, no sync needed');
  process.exit(0);
}

// 检查 API 侧的 client 是否包含 Collection 模型
const apiIndexPath = path.join(apiGeneratedDir, 'index.js');
if (!fs.existsSync(apiIndexPath)) {
  console.log('[sync-prisma] API generated client index.js not found, skipping');
  process.exit(0);
}

// 同步文件
let synced = 0;
const files = fs.readdirSync(apiGeneratedDir);
for (const file of files) {
  const srcPath = path.join(apiGeneratedDir, file);
  const dstPath = path.join(workerClientDir, file);
  const stat = fs.statSync(srcPath);
  if (stat.isFile()) {
    try {
      fs.copyFileSync(srcPath, dstPath);
      synced++;
    } catch (err) {
      // 可能被运行中的进程锁定
      console.warn(`[sync-prisma] Failed to copy ${file}: ${err.message}`);
    }
  }
}

console.log(`[sync-prisma] Synced ${synced}/${files.length} files from API to Worker client path`);
console.log(`[sync-prisma]   src: ${normalizedApi}`);
console.log(`[sync-prisma]   dst: ${normalizedWorker}`);

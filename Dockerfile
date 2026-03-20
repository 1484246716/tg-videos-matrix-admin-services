FROM node:25-bookworm-slim

WORKDIR /app

ENV NODE_ENV=development

# 1. 配置 Debian 镜像源并安装运行依赖（openssl + ffmpeg/ffprobe）
RUN sed -i 's/deb.debian.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list.d/debian.sources && \
    sed -i 's/security.debian.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list.d/debian.sources
RUN apt-get update -y && apt-get install -y openssl ffmpeg

# 2. 全局安装 pnpm 并配置国内镜像
RUN npm install -g pnpm --registry=https://registry.npmmirror.com
RUN pnpm config set registry https://registry.npmmirror.com

# 3. 仅拷贝包配置和 workspace 文件，利用 Docker 缓存加速安装
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml ./
COPY api/package.json ./api/
COPY worker/package.json ./worker/

# 4. 使用 pnpm 安装项目所有依赖
RUN pnpm install

# 5. 拷贝真正的业务代码 (此时因为有了 .dockerignore，宿主机的 node_modules 不会跟进去)
COPY api ./api
COPY worker ./worker

# 6. 生成 Prisma Client
RUN pnpm --filter @tg-crm/api exec prisma generate --schema=/app/api/prisma/schema.prisma

# 7. 构建 workspaces
RUN pnpm --filter @tg-crm/api run build
RUN pnpm --filter @tg-crm/worker run build

CMD ["pnpm", "--filter", "@tg-crm/api", "run", "start"]
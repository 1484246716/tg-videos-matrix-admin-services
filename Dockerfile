FROM node:25-bookworm-slim

WORKDIR /app

ENV NODE_ENV=development
ENV RELAY_LOCAL_PATH_LOCK_TTL_MS=120000

# 1. Configure Debian mirrors and runtime dependencies.
RUN sed -i 's/deb.debian.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list.d/debian.sources && \
    sed -i 's/security.debian.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list.d/debian.sources
RUN apt-get update -y && apt-get install -y openssl ffmpeg

# 2. Pin pnpm so Docker and local installs use the same behavior.
RUN corepack enable && corepack prepare pnpm@10.28.2 --activate
RUN pnpm config set registry https://registry.npmmirror.com

# 3. Copy workspace manifests first to maximize Docker layer cache hits.
COPY package.json pnpm-workspace.yaml pnpm-lock.yaml ./
COPY api/package.json ./api/
COPY worker/package.json ./worker/

# 4. Install dependencies from the lockfile.
RUN pnpm install --frozen-lockfile

# 5. Copy application source code.
COPY api ./api
COPY worker ./worker

# 6. Generate Prisma client.
RUN pnpm --filter @tg-crm/api exec prisma generate --schema=/app/api/prisma/schema.prisma

# 7. Build workspaces.
RUN pnpm --filter @tg-crm/api run build
RUN pnpm --filter @tg-crm/worker run build

CMD ["pnpm", "--filter", "@tg-crm/api", "run", "start"]

FROM node:25-bookworm-slim

WORKDIR /app

ENV NODE_ENV=development

COPY package*.json ./
COPY api/package*.json ./api/
COPY worker/package*.json ./worker/

RUN npm install --include=dev
RUN npm install --include=dev -w @tg-crm/api -w @tg-crm/worker

COPY api ./api
COPY worker ./worker

RUN npx --yes prisma generate --schema /app/api/prisma/schema.prisma
RUN npm run build -w @tg-crm/api
RUN npm run build -w @tg-crm/worker

CMD ["npm", "run", "start", "-w", "@tg-crm/api"]

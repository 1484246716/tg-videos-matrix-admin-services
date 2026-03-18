FROM node:25-bookworm-slim

WORKDIR /app

RUN apt-get update -y && apt-get install -y openssl

COPY package*.json ./
COPY api/package*.json ./api/
COPY worker/package*.json ./worker/

RUN npm install

COPY api ./api
COPY worker ./worker

RUN npm run prisma:generate -w @tg-crm/api
RUN npm run build -ws

CMD ["npm", "run", "start", "-w", "@tg-crm/api"]

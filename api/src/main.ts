import * as dotenv from 'dotenv';
import { resolve } from 'node:path';
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { NextFunction, Request, Response } from 'express';
import { AppModule } from './modules/app.module';
import { AppLogger, logger } from './logger';

dotenv.config({ path: resolve(__dirname, '..', '..', '.env') });

async function bootstrap() {
  const app = await NestFactory.create(AppModule, {
    logger: new AppLogger(),
  });

  app.setGlobalPrefix('api');
  app.enableCors({
    origin: (origin, callback) => {
      if (!origin) {
        callback(null, true);
        return;
      }

      const corsOrigin = (process.env.CORS_ORIGIN || '').trim();
      
      // 允许全部
      if (corsOrigin === '*') {
        callback(null, true);
        return;
      }

      // 处理逗号分隔的多个域名
      const allowedList = corsOrigin
        .split(',')
        .map((value) => value.trim())
        .filter(Boolean);

      if (allowedList.length > 0) {
        callback(null, allowedList.includes(origin));
        return;
      }

      // 默认放行本地开发
      const allowed =
        /^http:\/\/localhost:\d+$/.test(origin) ||
        /^http:\/\/127\.0\.0\.1:\d+$/.test(origin);

      callback(null, allowed);
    },
    // 👇 新增/强化的核心配置在这里
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE,OPTIONS', 
    allowedHeaders: 'Content-Type, Accept, Authorization, X-Requested-With',
    preflightContinue: false, 
    optionsSuccessStatus: 204,
    credentials: true,
  });

  app.use((req: Request, res: Response, next: NextFunction) => {
    const start = Date.now();

    res.on('finish', () => {
      logger.info('HTTP request completed', {
        context: 'HttpRequest',
        method: req.method,
        path: req.originalUrl || req.url,
        statusCode: res.statusCode,
        durationMs: Date.now() - start,
        ip: req.ip,
        userAgent: req.headers['user-agent'],
      });
    });

    next();
  });

  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );

  const port = Number(process.env.PORT || 3001);
  await app.listen(port);

  // eslint-disable-next-line no-console
  console.log(`API server running on http://localhost:${port}/api`);
}

bootstrap();

import 'dotenv/config';
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { AppModule } from './modules/app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  app.setGlobalPrefix('api');
  app.enableCors({
    origin: (origin, callback) => {
      if (!origin) {
        callback(null, true);
        return;
      }

      const corsOrigin = (process.env.CORS_ORIGIN || '').trim();
      if (corsOrigin === '*') {
        callback(null, true);
        return;
      }

      const allowedList = corsOrigin
        .split(',')
        .map((value) => value.trim())
        .filter(Boolean);

      if (allowedList.length > 0) {
        callback(null, allowedList.includes(origin));
        return;
      }

      const allowed =
        /^http:\/\/localhost:\d+$/.test(origin) ||
        /^http:\/\/127\.0\.0\.1:\d+$/.test(origin);

      callback(null, allowed);
    },
    credentials: true,
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

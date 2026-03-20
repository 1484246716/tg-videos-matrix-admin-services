import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { JwtModule } from '@nestjs/jwt';
import { PassportModule } from '@nestjs/passport';
import { PrismaModule } from '../prisma/prisma.module';
import { AuthController } from './auth.controller';
import { AuthService } from './auth.service';
import { PermissionController } from './permission.controller';
import { JwtAuthGuard } from './jwt-auth.guard';
import { JwtStrategy } from './jwt.strategy';
import { PermissionsGuard } from './permissions.guard';

@Module({
    imports: [
        PrismaModule,
        PassportModule,
        ConfigModule,
        JwtModule.registerAsync({
            inject: [ConfigService],
            useFactory: (configService: ConfigService) => ({
                secret: configService.get<string>('JWT_SECRET') || 'changeme-jwt-secret',
                signOptions: { expiresIn: 60 * 60 * 24 * 7 },
            }),
        }),
    ],
    controllers: [AuthController, PermissionController],
    providers: [AuthService, JwtStrategy, JwtAuthGuard, PermissionsGuard],
    exports: [JwtAuthGuard, PermissionsGuard, JwtModule],
})
export class AuthModule { }

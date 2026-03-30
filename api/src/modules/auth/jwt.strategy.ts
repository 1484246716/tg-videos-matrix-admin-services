import { Injectable, UnauthorizedException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PassportStrategy } from '@nestjs/passport';
import { ExtractJwt, Strategy } from 'passport-jwt';
import { PrismaService } from '../prisma/prisma.service';

export interface JwtPayload {
    sub: string;
    username: string;
    role: string;
    permissions?: string[];
}

@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
    constructor(
        configService: ConfigService,
        private readonly prisma: PrismaService,
    ) {
        super({
            jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
            ignoreExpiration: false,
            secretOrKey: configService.get<string>('JWT_SECRET') || 'changeme-jwt-secret',
        });
    }

    async validate(payload: JwtPayload) {
        const user = await this.prisma.user.findUnique({
            where: { id: BigInt(payload.sub) },
            select: {
                id: true,
                username: true,
                role: true,
                status: true,
                permissions: true,
            },
        });

        if (!user || user.status !== 'active') {
            throw new UnauthorizedException('账号不可用');
        }

        return {
            userId: user.id.toString(),
            username: user.username,
            role: user.role,
            permissions: user.permissions ?? [],
        };
    }
}

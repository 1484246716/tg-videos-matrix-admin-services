import { Injectable, UnauthorizedException } from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import * as bcrypt from 'bcryptjs';
import { PrismaService } from '../prisma/prisma.service';
import { LoginDto } from './dto/login.dto';

@Injectable()
export class AuthService {
    constructor(
        private readonly prisma: PrismaService,
        private readonly jwt: JwtService,
    ) { }

    async login(dto: LoginDto) {
        const user = await this.prisma.user.findUnique({
            where: { username: dto.username },
        });

        if (!user) {
            throw new UnauthorizedException('用户名或密码错误');
        }

        if (user.status !== 'active') {
            throw new UnauthorizedException('账号已被禁用');
        }

        const valid = await bcrypt.compare(dto.password, user.passwordHash);
        if (!valid) {
            throw new UnauthorizedException('用户名或密码错误');
        }

        await this.prisma.user.update({
            where: { id: user.id },
            data: { lastLoginAt: new Date() },
        });

        const payload = {
            sub: user.id.toString(),
            username: user.username,
            role: user.role,
            permissions: user.permissions ?? [],
        };

        const token = this.jwt.sign(payload);

        return {
            accessToken: token,
            user: {
                id: user.id.toString(),
                username: user.username,
                displayName: user.displayName,
                role: user.role,
                status: user.status,
                permissions: user.permissions ?? [],
            },
        };
    }

    async getMe(userId: string) {
        const user = await this.prisma.user.findUnique({
            where: { id: BigInt(userId) },
            select: {
                id: true,
                username: true,
                displayName: true,
                role: true,
                status: true,
                permissions: true,
                lastLoginAt: true,
                createdAt: true,
            },
        });

        if (!user) throw new UnauthorizedException('用户不存在');

        return {
            ...user,
            id: user.id.toString(),
        };
    }
}

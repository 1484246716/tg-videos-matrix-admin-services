import {
    BadRequestException,
    ConflictException,
    ForbiddenException,
    Injectable,
    NotFoundException,
} from '@nestjs/common';
import * as bcrypt from 'bcryptjs';
import { PrismaService } from '../prisma/prisma.service';
import { CreateUserDto } from './dto/create-user.dto';
import { UpdatePasswordDto, UpdateUserDto } from './dto/update-user.dto';

@Injectable()
export class UserService {
    constructor(private readonly prisma: PrismaService) { }

    private serialize<T>(value: T): T {
        return JSON.parse(
            JSON.stringify(value, (_k, v) => (typeof v === 'bigint' ? v.toString() : v)),
        ) as T;
    }

    async list() {
        const rows = await this.prisma.user.findMany({
            orderBy: { createdAt: 'desc' },
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
        return this.serialize(rows);
    }

    async create(dto: CreateUserDto) {
        if (!dto.password || dto.password.length < 8) {
            throw new BadRequestException('密码至少 8 位');
        }

        const exists = await this.prisma.user.findUnique({ where: { username: dto.username } });
        if (exists) throw new ConflictException('用户名已存在');

        const passwordHash = await bcrypt.hash(dto.password, 12);

        const role = dto.role ?? 'staff';
        const { DEFAULT_ROLE_PERMISSIONS } = await import('../auth/permission.config');
        const user = await this.prisma.user.create({
            data: {
                username: dto.username,
                passwordHash,
                displayName: dto.displayName,
                role,
                status: 'active',
                permissions: DEFAULT_ROLE_PERMISSIONS[role] ?? [],
            },
            select: {
                id: true,
                username: true,
                displayName: true,
                role: true,
                status: true,
                permissions: true,
                createdAt: true,
            },
        });

        return this.serialize(user);
    }

    async update(id: string, dto: UpdateUserDto) {
        const user = await this.prisma.user.findUnique({ where: { id: BigInt(id) } });
        if (!user) throw new NotFoundException('用户不存在');

        const { DEFAULT_ROLE_PERMISSIONS } = await import('../auth/permission.config');
        const updated = await this.prisma.user.update({
            where: { id: BigInt(id) },
            data: {
                displayName: dto.displayName,
                role: dto.role,
                status: dto.status,
                permissions: dto.role ? DEFAULT_ROLE_PERMISSIONS[dto.role] ?? user.permissions : user.permissions,
            },
            select: {
                id: true,
                username: true,
                displayName: true,
                role: true,
                status: true,
                permissions: true,
                updatedAt: true,
            },
        });

        return this.serialize(updated);
    }

    async updatePassword(
        id: string,
        dto: UpdatePasswordDto,
        requesterId: string,
        requesterRole: string,
    ) {
        const isSelf = id === requesterId;
        const isAdmin = requesterRole === 'admin';

        if (!isSelf && !isAdmin) {
            throw new ForbiddenException('无权修改他人密码');
        }

        if (!dto.newPassword || dto.newPassword.length < 8) {
            throw new BadRequestException('新密码至少 8 位');
        }

        const user = await this.prisma.user.findUnique({ where: { id: BigInt(id) } });
        if (!user) throw new NotFoundException('用户不存在');

        // Normal users must verify current password
        if (isSelf && !isAdmin) {
            if (!dto.currentPassword) throw new BadRequestException('请提供当前密码');
            const valid = await bcrypt.compare(dto.currentPassword, user.passwordHash);
            if (!valid) throw new BadRequestException('当前密码错误');
        }

        const passwordHash = await bcrypt.hash(dto.newPassword, 12);

        await this.prisma.user.update({
            where: { id: BigInt(id) },
            data: { passwordHash },
        });

        return { ok: true };
    }

    async remove(id: string) {
        const user = await this.prisma.user.findUnique({ where: { id: BigInt(id) } });
        if (!user) throw new NotFoundException('用户不存在');

        // soft delete via status
        await this.prisma.user.update({
            where: { id: BigInt(id) },
            data: { status: 'disabled' },
        });

        return { ok: true };
    }

    async updatePermissions(
        id: string,
        permissions: string[],
        requesterId: string,
    ) {
        if (id === requesterId) {
            throw new ForbiddenException('管理员不能修改自己的权限范围');
        }

        const user = await this.prisma.user.findUnique({ where: { id: BigInt(id) } });
        if (!user) throw new NotFoundException('用户不存在');

        const updated = await this.prisma.user.update({
            where: { id: BigInt(id) },
            data: {
                permissions,
            },
            select: {
                id: true,
                username: true,
                displayName: true,
                role: true,
                status: true,
                permissions: true,
                updatedAt: true,
            },
        });

        return this.serialize(updated);
    }
}

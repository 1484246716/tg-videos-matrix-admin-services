import {
    Body,
    Controller,
    Delete,
    Get,
    Param,
    Patch,
    Post,
    Put,
    Request,
    UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { Permissions } from '../auth/permissions.decorator';
import { PermissionsGuard } from '../auth/permissions.guard';
import { Roles } from '../auth/roles.decorator';
import { RolesGuard } from '../auth/roles.guard';
import { CreateUserDto } from './dto/create-user.dto';
import { UpdatePasswordDto, UpdateUserDto } from './dto/update-user.dto';
import { UserService } from './user.service';

interface AuthRequest {
    user: { userId: string; username: string; role: string };
}

@UseGuards(JwtAuthGuard, RolesGuard, PermissionsGuard)
@Controller('users')
export class UserController {
    constructor(private readonly userService: UserService) { }

    @Roles('admin')
    @Permissions('users:view')
    @Get()
    list() {
        return this.userService.list();
    }

    @Roles('admin')
    @Permissions('users:create')
    @Post()
    create(@Body() dto: CreateUserDto) {
        return this.userService.create(dto);
    }

    @Roles('admin')
    @Permissions('users:update')
    @Patch(':id')
    update(@Param('id') id: string, @Body() dto: UpdateUserDto) {
        return this.userService.update(id, dto);
    }

    @Permissions('users:update')
    @Patch(':id/password')
    updatePassword(
        @Param('id') id: string,
        @Body() dto: UpdatePasswordDto,
        @Request() req: AuthRequest,
    ) {
        return this.userService.updatePassword(id, dto, req.user.userId, req.user.role);
    }

    @Roles('admin')
    @Permissions('users:delete')
    @Delete(':id')
    remove(@Param('id') id: string) {
        return this.userService.remove(id);
    }

    @Roles('admin')
    @Permissions('roles:assign')
    @Put(':id/permissions')
    updatePermissions(
        @Param('id') id: string,
        @Body() dto: { permissions: string[] },
        @Request() req: AuthRequest,
    ) {
        return this.userService.updatePermissions(id, dto.permissions, req.user.userId);
    }
}

import { IsEnum, IsOptional, IsString, MinLength } from 'class-validator';

export class UpdateUserDto {
    @IsOptional()
    @IsString()
    displayName?: string;

    @IsOptional()
    @IsEnum(['admin', 'staff'])
    role?: 'admin' | 'staff';

    @IsOptional()
    @IsEnum(['active', 'disabled'])
    status?: 'active' | 'disabled';
}

export class UpdatePasswordDto {
    @IsString()
    @MinLength(8)
    newPassword!: string;

    @IsOptional()
    @IsString()
    currentPassword?: string;
}

import { IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

export class CreateBotDto {
  @IsString()
  name!: string;

  @IsString()
  tokenEncrypted!: string;

  @IsOptional()
  @IsString()
  tokenMasked?: string;

  @IsOptional()
  @IsInt()
  telegramBotId?: number;

  @IsOptional()
  @IsString()
  username?: string;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(120)
  rateLimitPerMin?: number;
}

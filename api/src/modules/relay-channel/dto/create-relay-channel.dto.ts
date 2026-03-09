import {
  IsBoolean,
  IsInt,
  IsOptional,
  IsString,
  Max,
  Min,
} from 'class-validator';

export class CreateRelayChannelDto {
  @IsString()
  name!: string;

  @IsString()
  tgChatId!: string;

  @IsInt()
  botId!: number;

  @IsOptional()
  @IsBoolean()
  isActive?: boolean;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(365)
  autoCleanupDays?: number;
}

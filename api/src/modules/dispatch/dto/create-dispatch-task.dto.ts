import { IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

export class CreateDispatchTaskDto {
  @IsString()
  channelId!: string;

  @IsString()
  mediaAssetId!: string;

  @IsOptional()
  @IsString()
  botId?: string;

  @IsString()
  scheduleSlot!: string;

  @IsString()
  plannedAt!: string;

  @IsString()
  nextRunAt!: string;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(1000)
  priority?: number;

  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(20)
  maxRetries?: number;
}

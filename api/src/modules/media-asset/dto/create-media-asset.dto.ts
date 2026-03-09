import { IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

export class CreateMediaAssetDto {
  @IsString()
  channelId!: string;

  @IsString()
  originalName!: string;

  @IsString()
  localPath!: string;

  @IsString()
  fileSize!: string;

  @IsString()
  fileHash!: string;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(86400)
  durationSec?: number;
}

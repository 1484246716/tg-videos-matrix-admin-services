import { IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

export class RenderCatalogPreviewDto {
  @IsString()
  channelId!: string;

  @IsString()
  catalogTemplateId!: string;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(200)
  recentLimitOverride?: number;
}

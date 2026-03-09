import { IsBoolean, IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

export class CreateCatalogTemplateDto {
  @IsString()
  name!: string;

  @IsString()
  bodyTemplate!: string;

  @IsOptional()
  @IsBoolean()
  isActive?: boolean;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(500)
  recentLimit?: number;
}

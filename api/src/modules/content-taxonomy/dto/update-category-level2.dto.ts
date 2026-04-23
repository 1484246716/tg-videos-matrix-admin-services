import { IsIn, IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

const TAXONOMY_STATUS_VALUES = ['active', 'inactive'] as const;

export class UpdateCategoryLevel2Dto {
  @IsOptional()
  @IsString()
  level1Id?: string;

  @IsOptional()
  @IsString()
  name?: string;

  @IsOptional()
  @IsString()
  slug?: string;

  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(100000)
  sort?: number;

  @IsOptional()
  @IsIn(TAXONOMY_STATUS_VALUES)
  status?: 'active' | 'inactive';
}

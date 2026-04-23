import { IsIn, IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

const TAXONOMY_STATUS_VALUES = ['active', 'inactive'] as const;

export class SaveCategoryLevel1Dto {
  @IsString()
  name!: string;

  @IsString()
  slug!: string;

  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(100000)
  sort?: number;

  @IsOptional()
  @IsIn(TAXONOMY_STATUS_VALUES)
  status?: 'active' | 'inactive';
}

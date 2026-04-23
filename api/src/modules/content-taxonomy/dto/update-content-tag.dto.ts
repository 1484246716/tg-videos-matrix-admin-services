import { IsIn, IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

const TAXONOMY_STATUS_VALUES = ['active', 'inactive'] as const;
const TAG_SCOPE_VALUES = ['public', 'adult_18', 'blocked'] as const;

export class UpdateContentTagDto {
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

  @IsOptional()
  @IsIn(TAG_SCOPE_VALUES)
  scope?: 'public' | 'adult_18' | 'blocked';
}

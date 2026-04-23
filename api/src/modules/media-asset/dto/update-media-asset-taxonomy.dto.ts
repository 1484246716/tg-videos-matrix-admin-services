import { IsArray, IsOptional, IsString } from 'class-validator';

export class UpdateMediaAssetTaxonomyDto {
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  level2Ids?: string[];

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  tagIds?: string[];
}

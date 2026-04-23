import { IsArray, IsOptional, IsString } from 'class-validator';

export class UpdateChannelDefaultTaxonomyDto {
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  level2Ids?: string[];

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  tagIds?: string[];
}

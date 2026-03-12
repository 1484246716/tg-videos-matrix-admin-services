import { IsArray, IsBoolean, IsIn, IsOptional, IsString, MaxLength } from 'class-validator';

export class CreateMessageTemplateDto {
  @IsString()
  @MaxLength(128)
  name!: string;

  @IsIn(['markdown', 'html', 'plain'])
  format!: 'markdown' | 'html' | 'plain';

  @IsString()
  content!: string;

  @IsOptional()
  @IsString()
  @MaxLength(512)
  imageUrl?: string;

  @IsOptional()
  buttons?: unknown;

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  variables?: string[];

  @IsOptional()
  @IsBoolean()
  isActive?: boolean;
}


import { IsBoolean, IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

export class CreateChannelDto {
  @IsString()
  name!: string;

  @IsString()
  tgChatId!: string;

  @IsOptional()
  @IsString()
  tgUsername?: string;

  @IsString()
  folderPath!: string;

  @IsOptional()
  @IsInt()
  @Min(30)
  @Max(3600)
  postIntervalSec?: number;

  @IsOptional()
  @IsString()
  defaultBotId?: string;

  @IsOptional()
  @IsInt()
  @Min(60)
  @Max(2592000)
  navIntervalSec?: number;

  @IsOptional()
  @IsString()
  aiSystemPromptTemplate?: string;

  @IsOptional()
  @IsBoolean()
  navEnabled?: boolean;

  @IsOptional()
  @IsString()
  navTemplateText?: string;

  @IsOptional()
  aiReplyMarkup?: unknown;

  @IsOptional()
  navReplyMarkup?: unknown;

  @IsOptional()
  tags?: string[];
}

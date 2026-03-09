import {
  IsBoolean,
  IsEnum,
  IsInt,
  IsOptional,
  IsString,
  Max,
  Min,
} from 'class-validator';
import { ChannelStatus } from '@prisma/client';

export class UpdateChannelDto {
  @IsOptional()
  @IsString()
  name?: string;

  @IsOptional()
  @IsString()
  tgChatId?: string;

  @IsOptional()
  @IsString()
  tgUsername?: string;

  @IsOptional()
  @IsString()
  folderPath?: string;

  @IsOptional()
  @IsEnum(ChannelStatus)
  status?: ChannelStatus;

  @IsOptional()
  @IsInt()
  postIntervalSec?: number;

  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(600)
  postJitterMinSec?: number;

  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(600)
  postJitterMaxSec?: number;

  @IsOptional()
  @IsInt()
  @Min(60)
  @Max(2592000)
  navIntervalSec?: number;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(1000)
  navRecentLimit?: number;

  @IsOptional()
  @IsBoolean()
  adEnabled?: boolean;

  @IsOptional()
  @IsBoolean()
  adPinEnabled?: boolean;

  @IsOptional()
  @IsString()
  alistTargetPath?: string;

  @IsOptional()
  @IsBoolean()
  autoImportEnabled?: boolean;

  @IsOptional()
  @IsString()
  defaultBotId?: string;

  @IsOptional()
  @IsString()
  relayChannelId?: string;

  @IsOptional()
  @IsString()
  aiModelProfileId?: string;

  @IsOptional()
  @IsString()
  catalogTemplateId?: string;

  @IsOptional()
  @IsString()
  aiSystemPromptTemplate?: string;

  @IsOptional()
  @IsString()
  navTemplateText?: string;

  @IsOptional()
  aiReplyMarkup?: unknown;

  @IsOptional()
  navReplyMarkup?: unknown;
}

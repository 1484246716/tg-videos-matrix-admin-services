import {
  IsArray,
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  MaxLength,
  Min,
  ValidateIf,
} from 'class-validator';

export class CreateMassMessageCampaignDto {
  @IsString()
  @MaxLength(128)
  name!: string;

  @IsOptional()
  @IsString()
  templateId?: string;

  @IsOptional()
  @IsString()
  contentOverride?: string;

  @IsOptional()
  @IsIn(['markdown', 'html', 'plain'])
  formatOverride?: 'markdown' | 'html' | 'plain';

  @IsOptional()
  @IsString()
  @MaxLength(512)
  imageUrlOverride?: string;

  @IsOptional()
  buttonsOverride?: unknown;

  @IsIn(['channel', 'group', 'mixed'])
  targetType!: 'channel' | 'group' | 'mixed';

  @IsArray()
  @IsString({ each: true })
  targetIds!: string[];

  @IsIn(['immediate', 'scheduled', 'recurring'])
  scheduleType!: 'immediate' | 'scheduled' | 'recurring';

  @IsOptional()
  @IsString()
  @MaxLength(64)
  timezone?: string;

  @ValidateIf((o) => o.scheduleType === 'scheduled' || o.scheduleType === 'recurring')
  @IsString()
  scheduledAt?: string; // ISO string

  @ValidateIf((o) => o.scheduleType === 'recurring')
  @IsOptional()
  recurringPattern?: unknown;

  @IsOptional()
  @IsInt()
  @Min(1)
  rateLimitPerMin?: number;

  @IsOptional()
  @IsInt()
  @Min(0)
  retryCount?: number;

  @IsOptional()
  @IsInt()
  @Min(1)
  retryIntervalSec?: number;

  @IsOptional()
  @IsIn(['none', 'pin_after_send', 'replace_pin'])
  pinMode?: 'none' | 'pin_after_send' | 'replace_pin';
}


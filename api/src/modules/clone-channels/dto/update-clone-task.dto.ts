import {
  IsArray,
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  Max,
  Min,
} from 'class-validator';

const TASK_STATUSES = [
  'draft',
  'running',
  'paused',
  'failed',
  'partial_success',
  'completed',
] as const;
const SCHEDULE_TYPES = ['once', 'hourly', 'daily'] as const;
const CRAWL_MODES = ['index_only', 'index_and_download'] as const;
const TARGET_PATH_TYPES = ['channel_path', 'collection_path'] as const;

export class UpdateCloneTaskDto {
  @IsOptional()
  @IsString()
  name?: string;

  @IsOptional()
  @IsIn(TASK_STATUSES)
  status?: (typeof TASK_STATUSES)[number];

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  channels?: string[];

  @IsOptional()
  @IsIn(SCHEDULE_TYPES)
  scheduleType?: (typeof SCHEDULE_TYPES)[number];

  @IsOptional()
  @IsString()
  scheduleCron?: string;

  @IsOptional()
  @IsString()
  timezone?: string;

  @IsOptional()
  @IsString()
  dailyRunTime?: string;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(5000)
  recentLimit?: number;

  @IsOptional()
  @IsIn(CRAWL_MODES)
  crawlMode?: (typeof CRAWL_MODES)[number];

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  contentTypes?: string[];

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(102400)
  downloadMaxFileMb?: number;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(64)
  globalDownloadConcurrency?: number;

  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(20)
  retryMax?: number;

  @IsOptional()
  @IsIn(TARGET_PATH_TYPES)
  targetPathType?: (typeof TARGET_PATH_TYPES)[number];

  @IsOptional()
  @IsString()
  targetPath?: string;
}

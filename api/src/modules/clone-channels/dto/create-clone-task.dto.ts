import {
  IsArray,
  IsIn,
  IsInt,
  IsOptional,
  IsString,
  Max,
  Min,
} from 'class-validator';

const SCHEDULE_TYPES = ['once', 'hourly', 'daily'] as const;
const CRAWL_MODES = ['index_only', 'index_and_download'] as const;
const TARGET_PATH_TYPES = ['channel_path', 'collection_path'] as const;

export class CreateCloneTaskDto {
  @IsString()
  name!: string;

  @IsArray()
  @IsString({ each: true })
  channels!: string[];

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
  @IsIn(CRAWL_MODES)
  crawlMode?: (typeof CRAWL_MODES)[number];

  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  contentTypes?: string[];

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(5000)
  recentLimit?: number;

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

  @IsIn(TARGET_PATH_TYPES)
  targetPathType!: (typeof TARGET_PATH_TYPES)[number];

  @IsString()
  targetPath!: string;
}

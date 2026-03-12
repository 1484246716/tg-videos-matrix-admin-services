import { IsBoolean, IsIn, IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

const TASK_TYPES = ['relay_upload', 'dispatch_send', 'catalog_publish', 'mass_message'] as const;
const RUN_INTERVAL_OPTIONS = [30, 120, 1800, 3600] as const;

export class UpdateTaskDefinitionDto {
  @IsOptional()
  @IsString()
  name?: string;

  @IsOptional()
  @IsIn(TASK_TYPES)
  taskType?: (typeof TASK_TYPES)[number];

  @IsOptional()
  @IsBoolean()
  isEnabled?: boolean;

  @IsOptional()
  @IsString()
  scheduleCron?: string;

  @IsOptional()
  @IsString()
  relayChannelId?: string;

  @IsOptional()
  @IsString()
  catalogTemplateId?: string;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(1000)
  priority?: number;

  @IsOptional()
  @IsInt()
  @Min(0)
  @Max(20)
  maxRetries?: number;

  @IsOptional()
  @IsIn(RUN_INTERVAL_OPTIONS)
  runIntervalSec?: (typeof RUN_INTERVAL_OPTIONS)[number];

  @IsOptional()
  payload?: unknown;
}

import { IsBoolean, IsIn, IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

const TASK_TYPES = ['relay_upload', 'dispatch_send', 'catalog_publish'] as const;

export class CreateTaskDefinitionDto {
  @IsString()
  name!: string;

  @IsIn(TASK_TYPES)
  taskType!: (typeof TASK_TYPES)[number];

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
  payload?: unknown;
}

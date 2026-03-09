import { IsIn, IsOptional, IsString } from 'class-validator';
import { TaskStatus } from '@prisma/client';

const TASK_STATUS_VALUES: TaskStatus[] = [
  'pending',
  'scheduled',
  'running',
  'success',
  'failed',
  'cancelled',
  'dead',
];

export class UpdateDispatchTaskStatusDto {
  @IsIn(TASK_STATUS_VALUES)
  status!: TaskStatus;

  @IsOptional()
  @IsString()
  telegramErrorCode?: string;

  @IsOptional()
  @IsString()
  telegramErrorMessage?: string;

  @IsOptional()
  @IsString()
  telegramMessageId?: string;

  @IsOptional()
  @IsString()
  telegramMessageLink?: string;
}

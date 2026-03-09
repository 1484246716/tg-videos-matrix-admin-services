import { IsIn } from 'class-validator';

const CHANNEL_STATUS_VALUES = ['active', 'paused', 'archived'] as const;

export class UpdateChannelStatusDto {
  @IsIn(CHANNEL_STATUS_VALUES)
  status!: 'active' | 'paused' | 'archived';
}

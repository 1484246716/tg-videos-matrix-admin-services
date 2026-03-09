import { IsEnum } from 'class-validator';
import { ChannelStatus } from '@prisma/client';

export class UpdateChannelStatusDto {
  @IsEnum(ChannelStatus)
  status!: ChannelStatus;
}

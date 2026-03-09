import { IsInt, IsOptional, IsString, Max, Min } from 'class-validator';

export class BatchEnqueueRelayUploadDto {
  @IsString()
  relayChannelId!: string;

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
}

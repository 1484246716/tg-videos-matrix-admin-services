import { IsIn, IsOptional, IsString } from 'class-validator';

export class UpdateMediaAssetStatusDto {
  @IsString()
  @IsIn(['new', 'ingesting', 'ready', 'relay_uploaded', 'failed', 'deleted'])
  status!: 'new' | 'ingesting' | 'ready' | 'relay_uploaded' | 'failed' | 'deleted';

  @IsOptional()
  @IsString()
  ingestError?: string;
}

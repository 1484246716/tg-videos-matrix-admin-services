import { IsBoolean, IsOptional, IsString } from 'class-validator';

export class PublishCatalogDto {
  @IsString()
  channelId!: string;

  @IsString()
  catalogTemplateId!: string;

  @IsOptional()
  @IsBoolean()
  pinAfterPublish?: boolean;
}

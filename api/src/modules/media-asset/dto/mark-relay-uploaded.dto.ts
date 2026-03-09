import { IsString } from 'class-validator';

export class MarkRelayUploadedDto {
  @IsString()
  telegramFileId!: string;

  @IsString()
  relayMessageId!: string;

  @IsString()
  telegramFileUniqueId!: string;
}

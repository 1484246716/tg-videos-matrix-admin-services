import {
  IsBoolean,
  IsInt,
  IsNumber,
  IsOptional,
  IsString,
  Max,
  Min,
} from 'class-validator';

export class CreateAiModelDto {
  @IsString()
  name!: string;

  @IsString()
  provider!: string;

  @IsString()
  model!: string;

  @IsString()
  apiKeyEncrypted!: string;

  @IsOptional()
  @IsString()
  endpointUrl?: string;

  @IsOptional()
  @IsString()
  systemPrompt?: string;

  @IsOptional()
  @IsString()
  captionPromptTemplate?: string;

  @IsOptional()
  @IsNumber()
  @Min(0)
  @Max(2)
  temperature?: number;

  @IsOptional()
  @IsNumber()
  @Min(0)
  @Max(1)
  topP?: number;

  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(32000)
  maxTokens?: number;

  @IsOptional()
  @IsInt()
  @Min(1000)
  @Max(120000)
  timeoutMs?: number;

  @IsOptional()
  @IsBoolean()
  isActive?: boolean;
}

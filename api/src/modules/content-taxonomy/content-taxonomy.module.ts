import { Global, Module } from '@nestjs/common';
import { ContentTaxonomyController } from './content-taxonomy.controller';
import { ContentTaxonomyService } from './content-taxonomy.service';

@Global()
@Module({
  controllers: [ContentTaxonomyController],
  providers: [ContentTaxonomyService],
  exports: [ContentTaxonomyService],
})
export class ContentTaxonomyModule {}

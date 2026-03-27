import { Module } from '@nestjs/common';
import { SearchController } from './search.controller';
import { SearchService } from './search.service';
import { SearchDocumentBuilder } from './search-document.builder';
import { SearchIndexOutboxService } from './search-index-outbox.service';
import { SearchIndexerService } from './search-indexer.service';
import { SearchOutboxScheduler } from './search-outbox.scheduler';

@Module({
  controllers: [SearchController],
  providers: [
    SearchService,
    SearchDocumentBuilder,
    SearchIndexOutboxService,
    SearchIndexerService,
    SearchOutboxScheduler,
  ],
  exports: [
    SearchService,
    SearchDocumentBuilder,
    SearchIndexOutboxService,
    SearchIndexerService,
  ],
})
export class SearchModule {}

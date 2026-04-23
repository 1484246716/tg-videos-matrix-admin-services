ALTER TABLE "clone_crawl_tasks"
ADD COLUMN "single_message_enabled" BOOLEAN NOT NULL DEFAULT false,
ADD COLUMN "single_message_link" VARCHAR(512);

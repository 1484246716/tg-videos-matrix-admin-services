-- CreateEnum
CREATE TYPE "CloneTaskStatus" AS ENUM ('draft', 'running', 'paused', 'failed', 'partial_success', 'completed');

-- CreateEnum
CREATE TYPE "CloneScheduleType" AS ENUM ('once', 'hourly', 'daily');

-- CreateEnum
CREATE TYPE "CloneCrawlMode" AS ENUM ('index_only', 'index_and_download');

-- CreateEnum
CREATE TYPE "CloneTargetPathType" AS ENUM ('channel_path', 'collection_path');

-- CreateEnum
CREATE TYPE "CloneChannelAccessStatus" AS ENUM ('ok', 'private', 'not_found', 'invalid');

-- CreateEnum
CREATE TYPE "CloneRunStatus" AS ENUM ('pending', 'running', 'success', 'failed', 'partial_success');

-- CreateEnum
CREATE TYPE "CloneDownloadStatus" AS ENUM ('none', 'queued', 'downloading', 'downloaded', 'failed_retryable', 'failed_final', 'paused_by_guard');

-- CreateTable
CREATE TABLE "clone_crawl_tasks" (
  "id" BIGSERIAL NOT NULL,
  "name" VARCHAR(128) NOT NULL,
  "status" "CloneTaskStatus" NOT NULL DEFAULT 'draft',
  "schedule_type" "CloneScheduleType" NOT NULL DEFAULT 'once',
  "schedule_cron" VARCHAR(64),
  "timezone" VARCHAR(64) NOT NULL DEFAULT 'Asia/Shanghai',
  "crawl_mode" "CloneCrawlMode" NOT NULL DEFAULT 'index_only',
  "content_types" TEXT[] DEFAULT ARRAY[]::TEXT[],
  "recent_limit" INTEGER NOT NULL DEFAULT 100,
  "download_max_file_mb" INTEGER,
  "global_download_concurrency" INTEGER NOT NULL DEFAULT 4,
  "retry_max" INTEGER NOT NULL DEFAULT 5,
  "target_path_type" "CloneTargetPathType" NOT NULL DEFAULT 'channel_path',
  "target_path" VARCHAR(512) NOT NULL,
  "created_by" BIGINT,
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMPTZ(6) NOT NULL,

  CONSTRAINT "clone_crawl_tasks_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "clone_crawl_task_channels" (
  "id" BIGSERIAL NOT NULL,
  "task_id" BIGINT NOT NULL,
  "channel_username" VARCHAR(128) NOT NULL,
  "channel_title" VARCHAR(255),
  "channel_access_status" "CloneChannelAccessStatus" NOT NULL DEFAULT 'ok',
  "last_fetched_message_id" BIGINT,
  "last_run_at" TIMESTAMPTZ(6),
  "last_error_code" VARCHAR(64),
  "last_error_message" TEXT,
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMPTZ(6) NOT NULL,

  CONSTRAINT "clone_crawl_task_channels_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "clone_crawl_runs" (
  "id" BIGSERIAL NOT NULL,
  "task_id" BIGINT NOT NULL,
  "status" "CloneRunStatus" NOT NULL DEFAULT 'pending',
  "started_at" TIMESTAMPTZ(6),
  "finished_at" TIMESTAMPTZ(6),
  "channel_total" INTEGER NOT NULL DEFAULT 0,
  "channel_success" INTEGER NOT NULL DEFAULT 0,
  "channel_failed" INTEGER NOT NULL DEFAULT 0,
  "indexed_count" INTEGER NOT NULL DEFAULT 0,
  "download_queued" INTEGER NOT NULL DEFAULT 0,
  "downloaded_count" INTEGER NOT NULL DEFAULT 0,
  "dedup_count" INTEGER NOT NULL DEFAULT 0,
  "disk_used_percent" DECIMAL(5,2),
  "inflight_bytes" BIGINT,
  "bandwidth_mbps" DECIMAL(10,2),
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMPTZ(6) NOT NULL,

  CONSTRAINT "clone_crawl_runs_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "clone_crawl_items" (
  "id" BIGSERIAL NOT NULL,
  "task_id" BIGINT NOT NULL,
  "run_id" BIGINT NOT NULL,
  "channel_username" VARCHAR(128) NOT NULL,
  "message_id" BIGINT NOT NULL,
  "message_date" TIMESTAMPTZ(6),
  "message_text" TEXT,
  "has_video" BOOLEAN NOT NULL DEFAULT false,
  "file_size" BIGINT,
  "mime_type" VARCHAR(64),
  "content_hash" VARCHAR(128),
  "local_path" VARCHAR(512),
  "download_status" "CloneDownloadStatus" NOT NULL DEFAULT 'none',
  "download_error_code" VARCHAR(64),
  "download_error" TEXT,
  "retry_count" INTEGER NOT NULL DEFAULT 0,
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMPTZ(6) NOT NULL,

  CONSTRAINT "clone_crawl_items_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "clone_crawl_tasks_status_updated_at_idx" ON "clone_crawl_tasks"("status", "updated_at" DESC);

-- CreateIndex
CREATE UNIQUE INDEX "clone_crawl_task_channels_task_id_channel_username_key" ON "clone_crawl_task_channels"("task_id", "channel_username");

-- CreateIndex
CREATE INDEX "clone_crawl_task_channels_task_id_channel_access_status_idx" ON "clone_crawl_task_channels"("task_id", "channel_access_status");

-- CreateIndex
CREATE INDEX "clone_crawl_runs_task_id_created_at_idx" ON "clone_crawl_runs"("task_id", "created_at" DESC);

-- CreateIndex
CREATE UNIQUE INDEX "clone_crawl_items_channel_username_message_id_key" ON "clone_crawl_items"("channel_username", "message_id");

-- CreateIndex
CREATE INDEX "clone_crawl_items_task_id_run_id_idx" ON "clone_crawl_items"("task_id", "run_id");

-- CreateIndex
CREATE INDEX "clone_crawl_items_download_status_idx" ON "clone_crawl_items"("download_status");

-- AddForeignKey
ALTER TABLE "clone_crawl_task_channels" ADD CONSTRAINT "clone_crawl_task_channels_task_id_fkey" FOREIGN KEY ("task_id") REFERENCES "clone_crawl_tasks"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "clone_crawl_runs" ADD CONSTRAINT "clone_crawl_runs_task_id_fkey" FOREIGN KEY ("task_id") REFERENCES "clone_crawl_tasks"("id") ON DELETE CASCADE ON UPDATE CASCADE;

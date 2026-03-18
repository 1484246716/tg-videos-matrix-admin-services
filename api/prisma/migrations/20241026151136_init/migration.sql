-- CreateSchema
CREATE SCHEMA IF NOT EXISTS "public";

-- CreateEnum
CREATE TYPE "UserStatus" AS ENUM ('active', 'disabled');

-- CreateEnum
CREATE TYPE "BotStatus" AS ENUM ('active', 'paused', 'blocked', 'invalid');

-- CreateEnum
CREATE TYPE "ChannelStatus" AS ENUM ('active', 'paused', 'archived');

-- CreateEnum
CREATE TYPE "MediaStatus" AS ENUM ('new', 'ingesting', 'ready', 'relay_uploaded', 'failed', 'deleted');

-- CreateEnum
CREATE TYPE "TaskStatus" AS ENUM ('pending', 'scheduled', 'running', 'success', 'failed', 'cancelled', 'dead');

-- CreateEnum
CREATE TYPE "SourceType" AS ENUM ('alist', 'manual', 'api');

-- CreateEnum
CREATE TYPE "CatalogTaskStatus" AS ENUM ('pending', 'running', 'success', 'failed', 'cancelled');

-- CreateEnum
CREATE TYPE "RiskLevel" AS ENUM ('low', 'medium', 'high', 'critical');

-- CreateEnum
CREATE TYPE "TaskDefinitionType" AS ENUM ('relay_upload', 'dispatch_send', 'catalog_publish', 'mass_message');

-- CreateEnum
CREATE TYPE "UserRole" AS ENUM ('admin', 'staff');

-- CreateEnum
CREATE TYPE "MassMessageCampaignStatus" AS ENUM ('draft', 'queued', 'running', 'completed', 'failed', 'paused', 'canceled');

-- CreateEnum
CREATE TYPE "MassMessageScheduleType" AS ENUM ('immediate', 'scheduled', 'recurring');

-- CreateEnum
CREATE TYPE "MassMessagePinMode" AS ENUM ('none', 'pin_after_send', 'replace_pin');

-- CreateEnum
CREATE TYPE "MassMessageContentFormat" AS ENUM ('markdown', 'html', 'plain');

-- CreateEnum
CREATE TYPE "MassMessageItemStatus" AS ENUM ('pending', 'scheduled', 'running', 'success', 'failed', 'cancelled', 'dead');

-- CreateTable
CREATE TABLE "users" (
                         "id" BIGSERIAL NOT NULL,
                         "username" VARCHAR(64) NOT NULL,
                         "email" VARCHAR(128),
                         "password_hash" VARCHAR(255) NOT NULL,
                         "display_name" VARCHAR(64),
                         "role" "UserRole" NOT NULL DEFAULT 'staff',
                         "status" "UserStatus" NOT NULL DEFAULT 'active',
                         "permissions" TEXT[] DEFAULT ARRAY[]::TEXT[],
                         "last_login_at" TIMESTAMPTZ(6),
                         "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                         "updated_at" TIMESTAMPTZ(6) NOT NULL,

                         CONSTRAINT "users_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "bots" (
                        "id" BIGSERIAL NOT NULL,
                        "name" VARCHAR(64) NOT NULL,
                        "token_encrypted" TEXT NOT NULL,
                        "token_masked" VARCHAR(64) NOT NULL,
                        "telegram_bot_id" BIGINT,
                        "username" VARCHAR(64),
                        "status" "BotStatus" NOT NULL DEFAULT 'active',
                        "rate_limit_per_min" INTEGER NOT NULL DEFAULT 8,
                        "daily_quota" INTEGER,
                        "last_health_check_at" TIMESTAMPTZ(6),
                        "last_error" TEXT,
                        "extra" JSONB,
                        "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        "updated_at" TIMESTAMPTZ(6) NOT NULL,

                        CONSTRAINT "bots_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "relay_channels" (
                                  "id" BIGSERIAL NOT NULL,
                                  "name" VARCHAR(128) NOT NULL,
                                  "tg_chat_id" BIGINT NOT NULL,
                                  "bot_id" BIGINT NOT NULL,
                                  "is_active" BOOLEAN NOT NULL DEFAULT true,
                                  "auto_cleanup_days" INTEGER NOT NULL DEFAULT 30,
                                  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                  "updated_at" TIMESTAMPTZ(6) NOT NULL,

                                  CONSTRAINT "relay_channels_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ai_model_profiles" (
                                     "id" BIGSERIAL NOT NULL,
                                     "name" VARCHAR(128) NOT NULL,
                                     "provider" VARCHAR(64) NOT NULL,
                                     "model" VARCHAR(128) NOT NULL,
                                     "api_key_encrypted" TEXT NOT NULL,
                                     "endpoint_url" VARCHAR(255),
                                     "system_prompt" TEXT,
                                     "caption_prompt_template" TEXT,
                                     "temperature" DECIMAL(4,2),
                                     "top_p" DECIMAL(4,2),
                                     "max_tokens" INTEGER,
                                     "timeout_ms" INTEGER NOT NULL DEFAULT 20000,
                                     "is_active" BOOLEAN NOT NULL DEFAULT true,
                                     "fallback_profile_id" BIGINT,
                                     "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                     "updated_at" TIMESTAMPTZ(6) NOT NULL,

                                     CONSTRAINT "ai_model_profiles_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "channels" (
                            "id" BIGSERIAL NOT NULL,
                            "name" VARCHAR(128) NOT NULL,
                            "tg_chat_id" VARCHAR(32) NOT NULL,
                            "tg_username" VARCHAR(64),
                            "folder_path" VARCHAR(255) NOT NULL,
                            "status" "ChannelStatus" NOT NULL DEFAULT 'active',
                            "default_bot_id" BIGINT,
                            "relay_channel_id" BIGINT,
                            "ai_model_profile_id" BIGINT,
                            "catalog_template_id" BIGINT,
                            "ai_system_prompt_template" TEXT,
                            "nav_template_text" TEXT,
                            "ai_reply_markup" JSONB,
                            "nav_reply_markup" JSONB,
                            "post_interval_sec" INTEGER NOT NULL DEFAULT 120,
                            "post_jitter_min_sec" INTEGER NOT NULL DEFAULT 5,
                            "post_jitter_max_sec" INTEGER NOT NULL DEFAULT 30,
                            "nav_interval_sec" INTEGER NOT NULL DEFAULT 604800,
                            "nav_recent_limit" INTEGER NOT NULL DEFAULT 60,
                            "nav_enabled" BOOLEAN NOT NULL DEFAULT false,
                            "ad_enabled" BOOLEAN NOT NULL DEFAULT false,
                            "ad_pin_enabled" BOOLEAN NOT NULL DEFAULT false,
                            "ad_message_id" BIGINT,
                            "last_ad_update_at" TIMESTAMPTZ(6),
                            "alist_target_path" VARCHAR(255),
                            "auto_import_enabled" BOOLEAN NOT NULL DEFAULT true,
                            "nav_message_id" BIGINT,
                            "created_by" BIGINT,
                            "last_post_at" TIMESTAMPTZ(6),
                            "last_nav_update_at" TIMESTAMPTZ(6),
                            "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            "updated_at" TIMESTAMPTZ(6) NOT NULL,

                            CONSTRAINT "channels_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "message_templates" (
                                     "id" BIGSERIAL NOT NULL,
                                     "name" VARCHAR(128) NOT NULL,
                                     "format" "MassMessageContentFormat" NOT NULL,
                                     "content" TEXT NOT NULL,
                                     "image_url" VARCHAR(512),
                                     "buttons" JSONB,
                                     "variables" TEXT[] DEFAULT ARRAY[]::TEXT[],
                                     "is_active" BOOLEAN NOT NULL DEFAULT true,
                                     "created_by" BIGINT,
                                     "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                     "updated_at" TIMESTAMPTZ(6) NOT NULL,

                                     CONSTRAINT "message_templates_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "mass_message_campaigns" (
                                          "id" BIGSERIAL NOT NULL,
                                          "name" VARCHAR(128) NOT NULL,
                                          "status" "MassMessageCampaignStatus" NOT NULL DEFAULT 'draft',
                                          "template_id" BIGINT,
                                          "content_override" TEXT,
                                          "format_override" "MassMessageContentFormat",
                                          "image_url_override" VARCHAR(512),
                                          "buttons_override" JSONB,
                                          "target_type" VARCHAR(16) NOT NULL,
                                          "target_ids" TEXT[],
                                          "schedule_type" "MassMessageScheduleType" NOT NULL,
                                          "timezone" VARCHAR(64) NOT NULL DEFAULT 'Asia/Shanghai',
                                          "scheduled_at" TIMESTAMPTZ(6),
                                          "recurring_pattern" JSONB,
                                          "rate_limit_per_min" INTEGER NOT NULL DEFAULT 10,
                                          "retry_count" INTEGER NOT NULL DEFAULT 3,
                                          "retry_interval_sec" INTEGER NOT NULL DEFAULT 30,
                                          "pin_mode" "MassMessagePinMode" NOT NULL DEFAULT 'none',
                                          "progress_total" INTEGER NOT NULL DEFAULT 0,
                                          "progress_sent" INTEGER NOT NULL DEFAULT 0,
                                          "progress_success" INTEGER NOT NULL DEFAULT 0,
                                          "progress_failed" INTEGER NOT NULL DEFAULT 0,
                                          "last_error" TEXT,
                                          "created_by" BIGINT,
                                          "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                          "updated_at" TIMESTAMPTZ(6) NOT NULL,

                                          CONSTRAINT "mass_message_campaigns_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "mass_message_items" (
                                      "id" BIGSERIAL NOT NULL,
                                      "campaign_id" BIGINT NOT NULL,
                                      "target_id" VARCHAR(64) NOT NULL,
                                      "target_type" VARCHAR(16) NOT NULL,
                                      "status" "MassMessageItemStatus" NOT NULL DEFAULT 'pending',
                                      "next_run_at" TIMESTAMPTZ(6) NOT NULL,
                                      "retry_count" INTEGER NOT NULL DEFAULT 0,
                                      "max_retries" INTEGER NOT NULL DEFAULT 3,
                                      "telegram_message_id" BIGINT,
                                      "telegram_message_link" TEXT,
                                      "telegram_error_code" VARCHAR(32),
                                      "telegram_error_message" TEXT,
                                      "pin_success" BOOLEAN,
                                      "pin_error_message" TEXT,
                                      "planned_at" TIMESTAMPTZ(6),
                                      "started_at" TIMESTAMPTZ(6),
                                      "finished_at" TIMESTAMPTZ(6),
                                      "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                      "updated_at" TIMESTAMPTZ(6) NOT NULL,

                                      CONSTRAINT "mass_message_items_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "media_assets" (
                                "id" BIGSERIAL NOT NULL,
                                "channel_id" BIGINT NOT NULL,
                                "original_name" VARCHAR(255) NOT NULL,
                                "local_path" VARCHAR(512) NOT NULL,
                                "archive_path" VARCHAR(512),
                                "file_size" BIGINT NOT NULL,
                                "file_hash" VARCHAR(128) NOT NULL,
                                "duration_sec" INTEGER,
                                "status" "MediaStatus" NOT NULL DEFAULT 'new',
                                "relay_message_id" BIGINT,
                                "telegram_file_id" TEXT,
                                "telegram_file_unique_id" TEXT,
                                "ai_generated_caption" TEXT,
                                "source_meta" JSONB,
                                "ingest_error" TEXT,
                                "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                "updated_at" TIMESTAMPTZ(6) NOT NULL,

                                CONSTRAINT "media_assets_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "catalog_templates" (
                                     "id" BIGSERIAL NOT NULL,
                                     "name" VARCHAR(128) NOT NULL,
                                     "body_template" TEXT NOT NULL,
                                     "recent_limit" INTEGER NOT NULL DEFAULT 60,
                                     "is_active" BOOLEAN NOT NULL DEFAULT true,
                                     "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                     "updated_at" TIMESTAMPTZ(6) NOT NULL,

                                     CONSTRAINT "catalog_templates_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "catalog_tasks" (
                                 "id" BIGSERIAL NOT NULL,
                                 "channel_id" BIGINT NOT NULL,
                                 "catalog_template_id" BIGINT NOT NULL,
                                 "status" "CatalogTaskStatus" NOT NULL DEFAULT 'pending',
                                 "planned_at" TIMESTAMPTZ(6),
                                 "started_at" TIMESTAMPTZ(6),
                                 "finished_at" TIMESTAMPTZ(6),
                                 "content_preview" TEXT,
                                 "pin_after_publish" BOOLEAN NOT NULL DEFAULT false,
                                 "pin_success" BOOLEAN,
                                 "pin_error_message" TEXT,
                                 "telegram_message_id" BIGINT,
                                 "telegram_message_link" TEXT,
                                 "error_message" TEXT,
                                 "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                 "updated_at" TIMESTAMPTZ(6) NOT NULL,

                                 CONSTRAINT "catalog_tasks_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "catalog_histories" (
                                     "id" BIGSERIAL NOT NULL,
                                     "channel_id" BIGINT NOT NULL,
                                     "catalog_template_id" BIGINT NOT NULL,
                                     "content" TEXT NOT NULL,
                                     "rendered_count" INTEGER NOT NULL DEFAULT 0,
                                     "published_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                     "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

                                     CONSTRAINT "catalog_histories_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "dispatch_tasks" (
                                  "id" BIGSERIAL NOT NULL,
                                  "channel_id" BIGINT NOT NULL,
                                  "media_asset_id" BIGINT NOT NULL,
                                  "bot_id" BIGINT,
                                  "schedule_slot" TIMESTAMPTZ(6) NOT NULL,
                                  "planned_at" TIMESTAMPTZ(6) NOT NULL,
                                  "next_run_at" TIMESTAMPTZ(6) NOT NULL,
                                  "reply_markup" JSONB,
                                  "caption" TEXT,
                                  "parse_mode" VARCHAR(16) DEFAULT 'HTML',
                                  "status" "TaskStatus" NOT NULL DEFAULT 'pending',
                                  "priority" INTEGER NOT NULL DEFAULT 100,
                                  "retry_count" INTEGER NOT NULL DEFAULT 0,
                                  "max_retries" INTEGER NOT NULL DEFAULT 6,
                                  "telegram_message_id" BIGINT,
                                  "telegram_message_link" TEXT,
                                  "telegram_error_code" VARCHAR(32),
                                  "telegram_error_message" TEXT,
                                  "started_at" TIMESTAMPTZ(6),
                                  "finished_at" TIMESTAMPTZ(6),
                                  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                  "updated_at" TIMESTAMPTZ(6) NOT NULL,

                                  CONSTRAINT "dispatch_tasks_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "dispatch_task_logs" (
                                      "id" BIGSERIAL NOT NULL,
                                      "dispatch_task_id" BIGINT NOT NULL,
                                      "action" VARCHAR(64) NOT NULL,
                                      "detail" JSONB,
                                      "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

                                      CONSTRAINT "dispatch_task_logs_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "risk_events" (
                               "id" BIGSERIAL NOT NULL,
                               "level" "RiskLevel" NOT NULL,
                               "event_type" VARCHAR(64) NOT NULL,
                               "bot_id" BIGINT,
                               "channel_id" BIGINT,
                               "dispatch_task_id" BIGINT,
                               "payload" JSONB,
                               "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

                               CONSTRAINT "risk_events_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "task_definitions" (
                                    "id" BIGSERIAL NOT NULL,
                                    "name" VARCHAR(128) NOT NULL,
                                    "task_type" "TaskDefinitionType" NOT NULL,
                                    "is_enabled" BOOLEAN NOT NULL DEFAULT true,
                                    "schedule_cron" VARCHAR(64),
                                    "relay_channel_id" BIGINT,
                                    "catalog_template_id" BIGINT,
                                    "priority" INTEGER NOT NULL DEFAULT 100,
                                    "max_retries" INTEGER NOT NULL DEFAULT 6,
                                    "run_interval_sec" INTEGER NOT NULL DEFAULT 1800,
                                    "next_run_at" TIMESTAMPTZ(6),
                                    "last_started_at" TIMESTAMPTZ(6),
                                    "payload" JSONB,
                                    "last_run_at" TIMESTAMPTZ(6),
                                    "last_run_status" VARCHAR(32),
                                    "last_run_summary" JSONB,
                                    "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                    "updated_at" TIMESTAMPTZ(6) NOT NULL,

                                    CONSTRAINT "task_definitions_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "users_username_key" ON "users"("username");

-- CreateIndex
CREATE UNIQUE INDEX "users_email_key" ON "users"("email");

-- CreateIndex
CREATE INDEX "bots_status_idx" ON "bots"("status");

-- CreateIndex
CREATE UNIQUE INDEX "relay_channels_tg_chat_id_key" ON "relay_channels"("tg_chat_id");

-- CreateIndex
CREATE INDEX "relay_channels_bot_id_idx" ON "relay_channels"("bot_id");

-- CreateIndex
CREATE INDEX "ai_model_profiles_is_active_idx" ON "ai_model_profiles"("is_active");

-- CreateIndex
CREATE UNIQUE INDEX "channels_tg_chat_id_key" ON "channels"("tg_chat_id");

-- CreateIndex
CREATE INDEX "channels_status_idx" ON "channels"("status");

-- CreateIndex
CREATE INDEX "channels_default_bot_id_status_idx" ON "channels"("default_bot_id", "status");

-- CreateIndex
CREATE INDEX "channels_catalog_template_id_idx" ON "channels"("catalog_template_id");

-- CreateIndex
CREATE INDEX "channels_created_by_updated_at_idx" ON "channels"("created_by", "updated_at" DESC);

-- CreateIndex
CREATE INDEX "message_templates_is_active_idx" ON "message_templates"("is_active");

-- CreateIndex
CREATE INDEX "message_templates_created_by_updated_at_idx" ON "message_templates"("created_by", "updated_at" DESC);

-- CreateIndex
CREATE INDEX "mass_message_campaigns_status_created_at_idx" ON "mass_message_campaigns"("status", "created_at" DESC);

-- CreateIndex
CREATE INDEX "mass_message_campaigns_schedule_type_scheduled_at_idx" ON "mass_message_campaigns"("schedule_type", "scheduled_at");

-- CreateIndex
CREATE INDEX "mass_message_campaigns_created_by_created_at_idx" ON "mass_message_campaigns"("created_by", "created_at" DESC);

-- CreateIndex
CREATE INDEX "mass_message_items_status_next_run_at_idx" ON "mass_message_items"("status", "next_run_at");

-- CreateIndex
CREATE INDEX "mass_message_items_campaign_id_created_at_idx" ON "mass_message_items"("campaign_id", "created_at" DESC);

-- CreateIndex
CREATE INDEX "mass_message_items_target_id_status_idx" ON "mass_message_items"("target_id", "status");

-- CreateIndex
CREATE INDEX "media_assets_channel_id_status_idx" ON "media_assets"("channel_id", "status");

-- CreateIndex
CREATE UNIQUE INDEX "media_assets_file_hash_file_size_key" ON "media_assets"("file_hash", "file_size");

-- CreateIndex
CREATE INDEX "catalog_templates_is_active_idx" ON "catalog_templates"("is_active");

-- CreateIndex
CREATE INDEX "catalog_tasks_channel_id_status_idx" ON "catalog_tasks"("channel_id", "status");

-- CreateIndex
CREATE INDEX "catalog_tasks_catalog_template_id_idx" ON "catalog_tasks"("catalog_template_id");

-- CreateIndex
CREATE INDEX "catalog_histories_channel_id_published_at_idx" ON "catalog_histories"("channel_id", "published_at" DESC);

-- CreateIndex
CREATE INDEX "catalog_histories_catalog_template_id_published_at_idx" ON "catalog_histories"("catalog_template_id", "published_at" DESC);

-- CreateIndex
CREATE INDEX "dispatch_tasks_status_next_run_at_idx" ON "dispatch_tasks"("status", "next_run_at");

-- CreateIndex
CREATE INDEX "dispatch_tasks_channel_id_status_idx" ON "dispatch_tasks"("channel_id", "status");

-- CreateIndex
CREATE UNIQUE INDEX "dispatch_tasks_channel_id_media_asset_id_schedule_slot_key" ON "dispatch_tasks"("channel_id", "media_asset_id", "schedule_slot");

-- CreateIndex
CREATE INDEX "dispatch_task_logs_dispatch_task_id_created_at_idx" ON "dispatch_task_logs"("dispatch_task_id", "created_at" DESC);

-- CreateIndex
CREATE INDEX "risk_events_created_at_idx" ON "risk_events"("created_at" DESC);

-- CreateIndex
CREATE INDEX "risk_events_bot_id_created_at_idx" ON "risk_events"("bot_id", "created_at" DESC);

-- CreateIndex
CREATE INDEX "task_definitions_task_type_is_enabled_idx" ON "task_definitions"("task_type", "is_enabled");

-- CreateIndex
CREATE INDEX "task_definitions_is_enabled_next_run_at_idx" ON "task_definitions"("is_enabled", "next_run_at");

-- AddForeignKey
ALTER TABLE "relay_channels" ADD CONSTRAINT "relay_channels_bot_id_fkey" FOREIGN KEY ("bot_id") REFERENCES "bots"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "channels" ADD CONSTRAINT "channels_default_bot_id_fkey" FOREIGN KEY ("default_bot_id") REFERENCES "bots"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "channels" ADD CONSTRAINT "channels_relay_channel_id_fkey" FOREIGN KEY ("relay_channel_id") REFERENCES "relay_channels"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "channels" ADD CONSTRAINT "channels_ai_model_profile_id_fkey" FOREIGN KEY ("ai_model_profile_id") REFERENCES "ai_model_profiles"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "channels" ADD CONSTRAINT "channels_created_by_fkey" FOREIGN KEY ("created_by") REFERENCES "users"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "channels" ADD CONSTRAINT "channels_catalog_template_id_fkey" FOREIGN KEY ("catalog_template_id") REFERENCES "catalog_templates"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "mass_message_items" ADD CONSTRAINT "mass_message_items_campaign_id_fkey" FOREIGN KEY ("campaign_id") REFERENCES "mass_message_campaigns"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "media_assets" ADD CONSTRAINT "media_assets_channel_id_fkey" FOREIGN KEY ("channel_id") REFERENCES "channels"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "catalog_tasks" ADD CONSTRAINT "catalog_tasks_channel_id_fkey" FOREIGN KEY ("channel_id") REFERENCES "channels"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "catalog_tasks" ADD CONSTRAINT "catalog_tasks_catalog_template_id_fkey" FOREIGN KEY ("catalog_template_id") REFERENCES "catalog_templates"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "catalog_histories" ADD CONSTRAINT "catalog_histories_channel_id_fkey" FOREIGN KEY ("channel_id") REFERENCES "channels"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "catalog_histories" ADD CONSTRAINT "catalog_histories_catalog_template_id_fkey" FOREIGN KEY ("catalog_template_id") REFERENCES "catalog_templates"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "dispatch_tasks" ADD CONSTRAINT "dispatch_tasks_channel_id_fkey" FOREIGN KEY ("channel_id") REFERENCES "channels"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "dispatch_tasks" ADD CONSTRAINT "dispatch_tasks_media_asset_id_fkey" FOREIGN KEY ("media_asset_id") REFERENCES "media_assets"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "dispatch_task_logs" ADD CONSTRAINT "dispatch_task_logs_dispatch_task_id_fkey" FOREIGN KEY ("dispatch_task_id") REFERENCES "dispatch_tasks"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "risk_events" ADD CONSTRAINT "risk_events_dispatch_task_id_fkey" FOREIGN KEY ("dispatch_task_id") REFERENCES "dispatch_tasks"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "task_definitions" ADD CONSTRAINT "task_definitions_relay_channel_id_fkey" FOREIGN KEY ("relay_channel_id") REFERENCES "relay_channels"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "task_definitions" ADD CONSTRAINT "task_definitions_catalog_template_id_fkey" FOREIGN KEY ("catalog_template_id") REFERENCES "catalog_templates"("id") ON DELETE SET NULL ON UPDATE CASCADE;


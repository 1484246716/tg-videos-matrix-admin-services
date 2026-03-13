-- TypeD: custom message management (templates + campaigns + items) + channel ad pin tracking

-- 1) Enums
DO $$ BEGIN
  CREATE TYPE "MassMessageCampaignStatus" AS ENUM (
    'draft', 'queued', 'running', 'completed', 'failed', 'paused', 'canceled'
  );
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
  CREATE TYPE "MassMessageScheduleType" AS ENUM ('immediate', 'scheduled', 'recurring');
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
  CREATE TYPE "MassMessagePinMode" AS ENUM ('none', 'pin_after_send', 'replace_pin');
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
  CREATE TYPE "MassMessageContentFormat" AS ENUM ('markdown', 'html', 'plain');
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

DO $$ BEGIN
  CREATE TYPE "MassMessageItemStatus" AS ENUM (
    'pending', 'scheduled', 'running', 'success', 'failed', 'cancelled', 'dead'
  );
EXCEPTION
  WHEN duplicate_object THEN null;
END $$;

-- 2) Channels: track bot-managed pinned ad message id
ALTER TABLE "channels"
  ADD COLUMN IF NOT EXISTS "ad_message_id" BIGINT,
  ADD COLUMN IF NOT EXISTS "last_ad_update_at" TIMESTAMPTZ(6);

-- 3) message_templates
CREATE TABLE IF NOT EXISTS "message_templates" (
  "id" BIGSERIAL PRIMARY KEY,
  "name" VARCHAR(128) NOT NULL,
  "format" "MassMessageContentFormat" NOT NULL,
  "content" TEXT NOT NULL,
  "image_url" VARCHAR(512),
  "buttons" JSONB,
  "variables" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  "is_active" BOOLEAN NOT NULL DEFAULT TRUE,
  "created_by" BIGINT,
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT NOW(),
  "updated_at" TIMESTAMPTZ(6) NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS "message_templates_is_active_idx" ON "message_templates" ("is_active");
CREATE INDEX IF NOT EXISTS "message_templates_created_by_updated_at_idx"
  ON "message_templates" ("created_by", "updated_at" DESC);

-- 4) mass_message_campaigns
CREATE TABLE IF NOT EXISTS "mass_message_campaigns" (
  "id" BIGSERIAL PRIMARY KEY,
  "name" VARCHAR(128) NOT NULL,
  "status" "MassMessageCampaignStatus" NOT NULL DEFAULT 'draft',

  "template_id" BIGINT,

  "content_override" TEXT,
  "format_override" "MassMessageContentFormat",
  "image_url_override" VARCHAR(512),
  "buttons_override" JSONB,

  "target_type" VARCHAR(16) NOT NULL,
  "target_ids" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],

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
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT NOW(),
  "updated_at" TIMESTAMPTZ(6) NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS "mass_message_campaigns_status_created_at_idx"
  ON "mass_message_campaigns" ("status", "created_at" DESC);
CREATE INDEX IF NOT EXISTS "mass_message_campaigns_schedule_type_scheduled_at_idx"
  ON "mass_message_campaigns" ("schedule_type", "scheduled_at");
CREATE INDEX IF NOT EXISTS "mass_message_campaigns_created_by_created_at_idx"
  ON "mass_message_campaigns" ("created_by", "created_at" DESC);

-- 5) mass_message_items
CREATE TABLE IF NOT EXISTS "mass_message_items" (
  "id" BIGSERIAL PRIMARY KEY,
  "campaign_id" BIGINT NOT NULL REFERENCES "mass_message_campaigns"("id") ON DELETE CASCADE,
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
  "created_at" TIMESTAMPTZ(6) NOT NULL DEFAULT NOW(),
  "updated_at" TIMESTAMPTZ(6) NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS "mass_message_items_status_next_run_at_idx"
  ON "mass_message_items" ("status", "next_run_at");
CREATE INDEX IF NOT EXISTS "mass_message_items_campaign_id_created_at_idx"
  ON "mass_message_items" ("campaign_id", "created_at" DESC);
CREATE INDEX IF NOT EXISTS "mass_message_items_target_id_status_idx"
  ON "mass_message_items" ("target_id", "status");

-- Add clone_crawl_accounts table for clone channel user-session login flow
-- Keep this migration idempotent for manual/partial applied environments.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'CloneAccountType') THEN
    CREATE TYPE "public"."CloneAccountType" AS ENUM ('user');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'CloneAccountStatus') THEN
    CREATE TYPE "public"."CloneAccountStatus" AS ENUM ('active', 'invalid', 'expired');
  END IF;
END$$;

CREATE TABLE IF NOT EXISTS "public"."clone_crawl_accounts" (
  "id" BIGSERIAL NOT NULL,
  "account_phone" VARCHAR(32) NOT NULL,
  "account_type" "public"."CloneAccountType" NOT NULL DEFAULT 'user',
  "session_string" TEXT NOT NULL,
  "status" "public"."CloneAccountStatus" NOT NULL DEFAULT 'active',
  "last_login_at" TIMESTAMP(6),
  "last_check_at" TIMESTAMP(6),
  "last_error_code" VARCHAR(64),
  "last_error_message" TEXT,
  "created_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT "clone_crawl_accounts_pkey" PRIMARY KEY ("id")
);

-- For environments where table existed with varchar columns, coerce to enum types.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'clone_crawl_accounts'
      AND column_name = 'account_type'
      AND udt_name <> 'CloneAccountType'
  ) THEN
    ALTER TABLE "public"."clone_crawl_accounts"
      ALTER COLUMN "account_type" TYPE "public"."CloneAccountType"
      USING "account_type"::"public"."CloneAccountType";
  END IF;
END$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'clone_crawl_accounts'
      AND column_name = 'status'
      AND udt_name <> 'CloneAccountStatus'
  ) THEN
    ALTER TABLE "public"."clone_crawl_accounts"
      ALTER COLUMN "status" TYPE "public"."CloneAccountStatus"
      USING "status"::"public"."CloneAccountStatus";
  END IF;
END$$;

CREATE UNIQUE INDEX IF NOT EXISTS "clone_crawl_accounts_account_phone_key"
ON "public"."clone_crawl_accounts"("account_phone");

CREATE INDEX IF NOT EXISTS "clone_crawl_accounts_status_updated_at_idx"
ON "public"."clone_crawl_accounts"("status", "updated_at" DESC);

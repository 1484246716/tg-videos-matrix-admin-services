-- AlterTable
ALTER TABLE "collections" ADD COLUMN "created_by" BIGINT;

-- Backfill from channel owner (staff-owned channels); admin-owned channels stay NULL (admin-only list)
UPDATE "collections" AS c
SET "created_by" = ch."created_by"
FROM "channels" AS ch
WHERE c."channel_id" = ch."id"
  AND ch."created_by" IS NOT NULL;

-- CreateIndex
CREATE INDEX "collections_created_by_updated_at_idx" ON "collections" ("created_by", "updated_at" DESC);

-- AddForeignKey
ALTER TABLE "collections" ADD CONSTRAINT "collections_created_by_fkey" FOREIGN KEY ("created_by") REFERENCES "users"("id") ON DELETE SET NULL ON UPDATE CASCADE;

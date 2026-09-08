-- Independent concurrent index build; exact definition/readiness checked at cutover.
CREATE INDEX CONCURRENTLY IF NOT EXISTS "portal_catalog_search_rows_latest_v2_idx" ON "private"."portal_catalog_search_rows_v2" USING "btree" ("dataset_kind", "id", "version" DESC, "modified_at" DESC, "state_code" DESC);

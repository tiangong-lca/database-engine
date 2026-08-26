CREATE INDEX "portal_catalog_search_rows_latest_v1_idx" ON "private"."portal_catalog_search_rows_v1" USING "btree" ("dataset_kind", "id", "version" DESC, "modified_at" DESC, "state_code" DESC);

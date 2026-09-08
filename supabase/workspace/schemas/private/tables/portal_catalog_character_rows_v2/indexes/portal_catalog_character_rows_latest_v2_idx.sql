CREATE INDEX "portal_catalog_character_rows_latest_v2_idx" ON "private"."portal_catalog_character_rows_v2" USING "btree" ("dataset_kind", "id", "version" DESC, "modified_at" DESC, "state_code" DESC);

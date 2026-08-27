CREATE INDEX "portal_sitemap_latest_shard_v1_idx" ON "private"."portal_sitemap_latest_rows_v1" USING "btree" ("shard_no", "dataset_kind", "id") INCLUDE ("version", "modified_at", "contract_version");

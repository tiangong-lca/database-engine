CREATE INDEX "lca_result_cache_lookup_idx" ON "public"."lca_result_cache" USING "btree" ("scope", "snapshot_id", "status", "updated_at" DESC);

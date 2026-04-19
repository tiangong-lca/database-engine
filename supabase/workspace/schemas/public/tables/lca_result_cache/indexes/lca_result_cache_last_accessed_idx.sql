CREATE INDEX "lca_result_cache_last_accessed_idx" ON "public"."lca_result_cache" USING "btree" ("last_accessed_at" DESC);

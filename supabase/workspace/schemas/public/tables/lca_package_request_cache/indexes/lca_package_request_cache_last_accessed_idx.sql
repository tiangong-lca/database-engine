CREATE INDEX "lca_package_request_cache_last_accessed_idx" ON "public"."lca_package_request_cache" USING "btree" ("last_accessed_at" DESC);

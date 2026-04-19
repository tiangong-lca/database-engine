CREATE INDEX "lca_package_request_cache_lookup_idx" ON "public"."lca_package_request_cache" USING "btree" ("requested_by", "operation", "status", "updated_at" DESC);

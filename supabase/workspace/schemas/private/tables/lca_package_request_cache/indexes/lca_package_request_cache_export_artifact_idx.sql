CREATE INDEX "lca_package_request_cache_export_artifact_idx" ON "private"."lca_package_request_cache" USING "btree" ("export_artifact_id") WHERE ("export_artifact_id" IS NOT NULL);

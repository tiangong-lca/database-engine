CREATE INDEX "lca_package_request_cache_report_artifact_idx" ON "public"."lca_package_request_cache" USING "btree" ("report_artifact_id") WHERE ("report_artifact_id" IS NOT NULL);

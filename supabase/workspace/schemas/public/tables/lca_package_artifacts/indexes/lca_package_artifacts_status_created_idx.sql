CREATE INDEX "lca_package_artifacts_status_created_idx" ON "public"."lca_package_artifacts" USING "btree" ("status", "created_at" DESC);

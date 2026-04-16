CREATE INDEX "lca_package_jobs_status_created_idx" ON "public"."lca_package_jobs" USING "btree" ("status", "created_at");

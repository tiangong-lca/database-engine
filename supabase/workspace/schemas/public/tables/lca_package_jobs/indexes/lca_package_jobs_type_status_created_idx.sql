CREATE INDEX "lca_package_jobs_type_status_created_idx" ON "public"."lca_package_jobs" USING "btree" ("job_type", "status", "created_at" DESC);

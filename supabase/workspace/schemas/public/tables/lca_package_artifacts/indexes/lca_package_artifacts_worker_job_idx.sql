CREATE INDEX "lca_package_artifacts_worker_job_idx" ON "public"."lca_package_artifacts" USING "btree" ("worker_job_id") WHERE ("worker_job_id" IS NOT NULL);

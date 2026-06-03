CREATE INDEX "lca_package_request_cache_worker_job_idx" ON "public"."lca_package_request_cache" USING "btree" ("worker_job_id") WHERE ("worker_job_id" IS NOT NULL);

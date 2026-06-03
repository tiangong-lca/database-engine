CREATE INDEX "lca_result_cache_worker_job_idx" ON "public"."lca_result_cache" USING "btree" ("worker_job_id") WHERE ("worker_job_id" IS NOT NULL);

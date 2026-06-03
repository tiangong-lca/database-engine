CREATE INDEX "lca_latest_all_unit_results_worker_job_idx" ON "public"."lca_latest_all_unit_results" USING "btree" ("worker_job_id") WHERE ("worker_job_id" IS NOT NULL);

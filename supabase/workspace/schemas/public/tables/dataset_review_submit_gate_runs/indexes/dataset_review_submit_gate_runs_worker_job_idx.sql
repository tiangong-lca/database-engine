CREATE INDEX "dataset_review_submit_gate_runs_worker_job_idx" ON "public"."dataset_review_submit_gate_runs" USING "btree" ("worker_job_id") WHERE ("worker_job_id" IS NOT NULL);

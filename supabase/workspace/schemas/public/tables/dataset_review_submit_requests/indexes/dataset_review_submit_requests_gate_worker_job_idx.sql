CREATE INDEX "dataset_review_submit_requests_gate_worker_job_idx" ON "public"."dataset_review_submit_requests" USING "btree" ("gate_worker_job_id") WHERE ("gate_worker_job_id" IS NOT NULL);

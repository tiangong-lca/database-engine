CREATE INDEX "dataset_review_submit_requests_submit_worker_job_idx" ON "private"."dataset_review_submit_requests" USING "btree" ("submit_worker_job_id") WHERE ("submit_worker_job_id" IS NOT NULL);

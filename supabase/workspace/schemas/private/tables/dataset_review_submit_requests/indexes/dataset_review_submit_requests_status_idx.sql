CREATE INDEX "dataset_review_submit_requests_status_idx" ON "private"."dataset_review_submit_requests" USING "btree" ("status", "modified_at", "created_at");

CREATE INDEX "dataset_review_submit_requests_gate_run_idx" ON "public"."dataset_review_submit_requests" USING "btree" ("gate_run_id") WHERE ("gate_run_id" IS NOT NULL);

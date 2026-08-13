CREATE INDEX "dataset_review_submit_gate_runs_requested_by_idx" ON "private"."dataset_review_submit_gate_runs" USING "btree" ("requested_by", "created_at" DESC);

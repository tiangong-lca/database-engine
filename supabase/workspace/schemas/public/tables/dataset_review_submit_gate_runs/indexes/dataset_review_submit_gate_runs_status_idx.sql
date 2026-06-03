CREATE INDEX "dataset_review_submit_gate_runs_status_idx" ON "public"."dataset_review_submit_gate_runs" USING "btree" ("status", "modified_at" DESC);

CREATE INDEX "dataset_review_submit_gate_runs_supersedes_idx" ON "public"."dataset_review_submit_gate_runs" USING "btree" ("supersedes_gate_run_id") WHERE ("supersedes_gate_run_id" IS NOT NULL);

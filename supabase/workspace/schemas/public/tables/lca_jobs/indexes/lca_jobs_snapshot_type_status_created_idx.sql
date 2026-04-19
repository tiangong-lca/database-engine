CREATE INDEX "lca_jobs_snapshot_type_status_created_idx" ON "public"."lca_jobs" USING "btree" ("snapshot_id", "job_type", "status", "created_at" DESC);

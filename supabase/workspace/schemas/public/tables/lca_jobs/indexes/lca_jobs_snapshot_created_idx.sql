CREATE INDEX "lca_jobs_snapshot_created_idx" ON "public"."lca_jobs" USING "btree" ("snapshot_id", "created_at" DESC);

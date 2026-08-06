CREATE INDEX "lca_results_snapshot_created_idx" ON "private"."lca_results" USING "btree" ("snapshot_id", "created_at" DESC);

CREATE INDEX "lca_release_runs_status_created_idx" ON "private"."lca_release_runs" USING "btree" ("status", "created_at" DESC);

CREATE INDEX "lca_release_approvals_run_idx" ON "private"."lca_release_approvals" USING "btree" ("release_run_id", "approved_at" DESC);

CREATE INDEX "lca_snapshot_gc_run_items_status_idx" ON "private"."lca_snapshot_gc_run_items" USING "btree" ("action_status", "created_at" DESC);

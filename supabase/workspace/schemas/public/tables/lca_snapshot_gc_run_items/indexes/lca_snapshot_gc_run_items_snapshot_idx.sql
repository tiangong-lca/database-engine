CREATE INDEX "lca_snapshot_gc_run_items_snapshot_idx" ON "public"."lca_snapshot_gc_run_items" USING "btree" ("snapshot_id") WHERE ("snapshot_id" IS NOT NULL);

CREATE INDEX "lca_snapshot_gc_runs_started_idx" ON "private"."lca_snapshot_gc_runs" USING "btree" ("started_at" DESC);

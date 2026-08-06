CREATE INDEX "lca_network_snapshots_status_created_idx" ON "private"."lca_network_snapshots" USING "btree" ("status", "created_at" DESC);

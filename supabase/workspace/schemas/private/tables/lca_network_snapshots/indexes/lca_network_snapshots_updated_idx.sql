CREATE INDEX "lca_network_snapshots_updated_idx" ON "private"."lca_network_snapshots" USING "btree" ("updated_at" DESC);

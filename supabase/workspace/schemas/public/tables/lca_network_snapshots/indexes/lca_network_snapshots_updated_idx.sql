CREATE INDEX "lca_network_snapshots_updated_idx" ON "public"."lca_network_snapshots" USING "btree" ("updated_at" DESC);

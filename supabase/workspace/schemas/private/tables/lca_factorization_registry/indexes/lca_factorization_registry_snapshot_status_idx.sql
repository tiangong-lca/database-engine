CREATE INDEX "lca_factorization_registry_snapshot_status_idx" ON "private"."lca_factorization_registry" USING "btree" ("snapshot_id", "status", "updated_at" DESC);

CREATE INDEX "lca_snapshot_artifacts_certificate_lookup_idx" ON "private"."lca_snapshot_artifacts" USING "btree" ("snapshot_id", "status", "snapshot_index_sha256");

CREATE INDEX "lca_snapshot_artifacts_snapshot_status_idx" ON "public"."lca_snapshot_artifacts" USING "btree" ("snapshot_id", "status", "created_at" DESC);

CREATE UNIQUE INDEX "lca_snapshot_artifacts_snapshot_format_key" ON "public"."lca_snapshot_artifacts" USING "btree" ("snapshot_id", "artifact_format");

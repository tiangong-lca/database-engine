CREATE INDEX "lca_snapshot_artifacts_created_idx" ON "public"."lca_snapshot_artifacts" USING "btree" ("created_at" DESC);

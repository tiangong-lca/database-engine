CREATE INDEX "lcia_scope_closure_checks_snapshot_artifact_idx" ON "private"."lcia_scope_closure_checks" USING "btree" ("snapshot_artifact_id") WHERE ("snapshot_artifact_id" IS NOT NULL);

CREATE INDEX "lcia_scope_closure_checks_snapshot_idx" ON "private"."lcia_scope_closure_checks" USING "btree" ("snapshot_id") WHERE ("snapshot_id" IS NOT NULL);

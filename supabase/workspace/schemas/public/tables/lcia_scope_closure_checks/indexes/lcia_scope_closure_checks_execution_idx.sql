CREATE INDEX "lcia_scope_closure_checks_execution_idx" ON "public"."lcia_scope_closure_checks" USING "btree" ("scan_execution_id", "created_at" DESC, "id" DESC);

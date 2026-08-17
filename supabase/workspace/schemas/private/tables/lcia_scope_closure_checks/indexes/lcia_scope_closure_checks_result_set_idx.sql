CREATE INDEX "lcia_scope_closure_checks_result_set_idx" ON "private"."lcia_scope_closure_checks" USING "btree" ("result_set_id") WHERE ("result_set_id" IS NOT NULL);

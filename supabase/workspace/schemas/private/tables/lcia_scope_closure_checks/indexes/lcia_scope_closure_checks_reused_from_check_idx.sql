CREATE INDEX "lcia_scope_closure_checks_reused_from_check_idx" ON "private"."lcia_scope_closure_checks" USING "btree" ("reused_from_check_id") WHERE ("reused_from_check_id" IS NOT NULL);

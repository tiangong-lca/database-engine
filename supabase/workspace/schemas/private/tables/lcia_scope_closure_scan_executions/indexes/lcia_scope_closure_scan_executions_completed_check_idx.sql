CREATE INDEX "lcia_scope_closure_scan_executions_completed_check_idx" ON "private"."lcia_scope_closure_scan_executions" USING "btree" ("completed_check_id") WHERE ("completed_check_id" IS NOT NULL);

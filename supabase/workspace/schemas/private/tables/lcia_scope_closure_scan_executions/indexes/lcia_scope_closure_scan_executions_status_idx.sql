CREATE INDEX "lcia_scope_closure_scan_executions_status_idx" ON "private"."lcia_scope_closure_scan_executions" USING "btree" ("status", "lease_expires_at", "created_at");

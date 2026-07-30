CREATE INDEX "lcia_scope_closure_scan_executions_leased_by_job_idx" ON "public"."lcia_scope_closure_scan_executions" USING "btree" ("leased_by_job_id") WHERE ("leased_by_job_id" IS NOT NULL);

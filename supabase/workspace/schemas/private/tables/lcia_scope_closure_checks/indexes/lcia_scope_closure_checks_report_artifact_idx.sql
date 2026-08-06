CREATE INDEX "lcia_scope_closure_checks_report_artifact_idx" ON "private"."lcia_scope_closure_checks" USING "btree" ("report_artifact_id") WHERE ("report_artifact_id" IS NOT NULL);

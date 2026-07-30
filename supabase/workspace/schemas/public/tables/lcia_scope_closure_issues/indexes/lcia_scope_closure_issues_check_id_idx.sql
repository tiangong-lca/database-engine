CREATE INDEX "lcia_scope_closure_issues_check_id_idx" ON "public"."lcia_scope_closure_issues" USING "btree" ("closure_check_id", "severity", "issue_code", "id");

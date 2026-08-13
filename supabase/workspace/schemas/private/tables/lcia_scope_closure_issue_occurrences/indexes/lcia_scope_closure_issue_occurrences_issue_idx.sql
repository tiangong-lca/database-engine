CREATE INDEX "lcia_scope_closure_issue_occurrences_issue_idx" ON "private"."lcia_scope_closure_issue_occurrences" USING "btree" ("closure_issue_id", "id");

CREATE UNIQUE INDEX "lca_release_approvals_active_uidx" ON "public"."lca_release_approvals" USING "btree" ("release_run_id") WHERE ("status" = 'approved'::"text");

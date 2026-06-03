CREATE INDEX "lca_results_expires_at_idx" ON "public"."lca_results" USING "btree" ("expires_at", "created_at") WHERE ("is_pinned" = false);

CREATE INDEX "lca_latest_all_unit_results_computed_idx" ON "private"."lca_latest_all_unit_results" USING "btree" ("computed_at" DESC);

CREATE UNIQUE INDEX "lca_result_cache_result_uidx" ON "private"."lca_result_cache" USING "btree" ("result_id") WHERE ("result_id" IS NOT NULL);

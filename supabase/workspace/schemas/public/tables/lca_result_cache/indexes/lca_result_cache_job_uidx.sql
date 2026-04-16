CREATE UNIQUE INDEX "lca_result_cache_job_uidx" ON "public"."lca_result_cache" USING "btree" ("job_id") WHERE ("job_id" IS NOT NULL);

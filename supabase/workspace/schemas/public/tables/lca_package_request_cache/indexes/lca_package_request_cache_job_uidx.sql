CREATE UNIQUE INDEX "lca_package_request_cache_job_uidx" ON "public"."lca_package_request_cache" USING "btree" ("job_id") WHERE ("job_id" IS NOT NULL);

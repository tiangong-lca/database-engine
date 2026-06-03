CREATE INDEX "lca_factorization_registry_prepared_worker_job_idx" ON "public"."lca_factorization_registry" USING "btree" ("prepared_worker_job_id") WHERE ("prepared_worker_job_id" IS NOT NULL);

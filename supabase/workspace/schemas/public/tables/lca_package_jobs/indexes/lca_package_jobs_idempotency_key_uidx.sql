CREATE UNIQUE INDEX "lca_package_jobs_idempotency_key_uidx" ON "public"."lca_package_jobs" USING "btree" ("idempotency_key") WHERE ("idempotency_key" IS NOT NULL);

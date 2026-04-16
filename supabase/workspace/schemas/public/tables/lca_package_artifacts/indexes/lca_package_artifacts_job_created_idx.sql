CREATE INDEX "lca_package_artifacts_job_created_idx" ON "public"."lca_package_artifacts" USING "btree" ("job_id", "created_at" DESC);

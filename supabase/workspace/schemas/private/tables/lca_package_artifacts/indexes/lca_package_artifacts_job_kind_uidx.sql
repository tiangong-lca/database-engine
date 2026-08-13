CREATE UNIQUE INDEX "lca_package_artifacts_job_kind_uidx" ON "private"."lca_package_artifacts" USING "btree" ("job_id", "artifact_kind");

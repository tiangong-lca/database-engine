CREATE INDEX "lca_package_export_items_worker_job_idx" ON "public"."lca_package_export_items" USING "btree" ("worker_job_id") WHERE ("worker_job_id" IS NOT NULL);

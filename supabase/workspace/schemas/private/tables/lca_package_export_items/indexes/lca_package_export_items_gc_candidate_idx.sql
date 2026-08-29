CREATE INDEX "lca_package_export_items_gc_candidate_idx" ON "private"."lca_package_export_items" USING "btree" ("created_at", "id") INCLUDE ("worker_job_id", "job_id");

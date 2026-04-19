CREATE INDEX "lca_package_export_items_job_refs_idx" ON "public"."lca_package_export_items" USING "btree" ("job_id", "refs_done", "created_at", "table_name");

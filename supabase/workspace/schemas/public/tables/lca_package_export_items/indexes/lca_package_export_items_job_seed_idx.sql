CREATE INDEX "lca_package_export_items_job_seed_idx" ON "public"."lca_package_export_items" USING "btree" ("job_id", "is_seed", "created_at");

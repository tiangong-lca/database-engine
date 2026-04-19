CREATE UNIQUE INDEX "lca_package_export_items_job_dataset_uidx" ON "public"."lca_package_export_items" USING "btree" ("job_id", "table_name", "dataset_id", "version");

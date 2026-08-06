CREATE INDEX "lca_release_dataset_lookup_idx" ON "private"."lca_release_dataset_versions" USING "btree" ("dataset_type", "dataset_uuid", "dataset_version");

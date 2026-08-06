CREATE UNIQUE INDEX "worker_legacy_job_table_rows_source_uidx" ON "archive"."worker_legacy_job_table_rows" USING "btree" ("source_table", "source_row_id") WHERE ("source_row_id" IS NOT NULL);

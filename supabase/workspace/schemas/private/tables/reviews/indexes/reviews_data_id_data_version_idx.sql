CREATE INDEX "reviews_data_id_data_version_idx" ON "private"."reviews" USING "btree" ("data_id", "data_version");

CREATE INDEX "reviews_data_id_data_version_idx" ON "public"."reviews" USING "btree" ("data_id", "data_version");

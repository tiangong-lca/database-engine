CREATE UNIQUE INDEX "dataset_derivative_rebuild_batch_ordinal_uidx" ON "util"."dataset_derivative_rebuild_requests" USING "btree" ("batch_id", "batch_ordinal") WHERE ("batch_id" IS NOT NULL);

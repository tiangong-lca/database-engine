CREATE INDEX "lcia_result_packages_latest_all_unit_result_idx" ON "private"."lcia_result_packages" USING "btree" ("latest_all_unit_result_id") WHERE ("latest_all_unit_result_id" IS NOT NULL);

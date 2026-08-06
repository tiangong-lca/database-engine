CREATE INDEX "lcia_result_packages_closure_check_idx" ON "private"."lcia_result_packages" USING "btree" ("closure_check_id", "created_at" DESC);

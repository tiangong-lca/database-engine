CREATE INDEX "lcia_result_sets_created_idx" ON "private"."lcia_result_sets" USING "btree" ("created_at" DESC, "id" DESC);

CREATE INDEX "lcia_result_publications_package_idx" ON "private"."lcia_result_publications" USING "btree" ("package_id", "created_at" DESC);

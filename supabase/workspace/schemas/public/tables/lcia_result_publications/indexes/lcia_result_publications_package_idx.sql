CREATE INDEX "lcia_result_publications_package_idx" ON "public"."lcia_result_publications" USING "btree" ("package_id", "created_at" DESC);

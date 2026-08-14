CREATE INDEX "lcia_result_publications_list_idx" ON "private"."lcia_result_publications" USING "btree" ("is_current" DESC, "published_at" DESC NULLS LAST, "created_at" DESC);

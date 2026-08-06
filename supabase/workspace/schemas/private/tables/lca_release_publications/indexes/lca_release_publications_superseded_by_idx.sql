CREATE INDEX "lca_release_publications_superseded_by_idx" ON "private"."lca_release_publications" USING "btree" ("superseded_by") WHERE ("superseded_by" IS NOT NULL);

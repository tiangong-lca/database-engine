CREATE UNIQUE INDEX "lca_release_publications_actor_idempotency_uidx" ON "private"."lca_release_publications" USING "btree" ("executed_by", "idempotency_key");

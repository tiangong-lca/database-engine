CREATE INDEX "dataset_alias_execution_preflight_actor_read_idx" ON "util"."dataset_alias_execution_preflights" USING "btree" ("actor_user_id", "completed_at" DESC);

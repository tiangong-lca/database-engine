CREATE UNIQUE INDEX "dataset_alias_execution_preflight_approval_uidx" ON "util"."dataset_alias_execution_preflights" USING "btree" ("actor_user_id", "approval_identity_sha256");

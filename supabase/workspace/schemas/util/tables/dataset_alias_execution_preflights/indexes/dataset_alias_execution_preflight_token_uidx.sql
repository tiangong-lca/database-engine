CREATE UNIQUE INDEX "dataset_alias_execution_preflight_token_uidx" ON "util"."dataset_alias_execution_preflights" USING "btree" ("token_sha256");

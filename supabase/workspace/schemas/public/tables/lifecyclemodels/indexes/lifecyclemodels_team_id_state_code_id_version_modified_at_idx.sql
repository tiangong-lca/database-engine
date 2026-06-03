CREATE INDEX "lifecyclemodels_team_id_state_code_id_version_modified_at_idx" ON "public"."lifecyclemodels" USING "btree" ("team_id", "state_code", "id", "version" DESC, "modified_at" DESC);

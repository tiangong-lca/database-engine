CREATE INDEX "processes_user_id_state_code_id_version_modified_at_idx" ON "public"."processes" USING "btree" ("user_id", "state_code", "id", "version" DESC, "modified_at" DESC);

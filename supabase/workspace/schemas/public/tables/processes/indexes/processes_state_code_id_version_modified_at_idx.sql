CREATE INDEX "processes_state_code_id_version_modified_at_idx" ON "public"."processes" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);

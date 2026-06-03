CREATE INDEX "flows_state_code_id_version_modified_at_idx" ON "public"."flows" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);

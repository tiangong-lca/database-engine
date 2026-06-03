CREATE INDEX "lifecyclemodels_state_code_id_version_modified_at_idx" ON "public"."lifecyclemodels" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);

CREATE INDEX "flowproperties_state_code_id_version_modified_at_idx" ON "public"."flowproperties" USING "btree" ("state_code", "id", "version" DESC, "modified_at" DESC);

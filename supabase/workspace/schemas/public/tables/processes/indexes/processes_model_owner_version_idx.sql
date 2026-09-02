CREATE INDEX "processes_model_owner_version_idx" ON "public"."processes" USING "btree" ("model_id", COALESCE("model_version", "version")) WHERE ("model_id" IS NOT NULL);

CREATE INDEX "processes_model_id_version_idx" ON "public"."processes" USING "btree" ("model_id", "version") WHERE ("model_id" IS NOT NULL);

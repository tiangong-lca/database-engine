CREATE INDEX "processes_user_id_created_at_idx" ON "public"."processes" USING "btree" ("user_id", "created_at" DESC);

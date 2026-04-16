CREATE INDEX "flows_user_id_created_at_idx" ON "public"."flows" USING "btree" ("user_id", "created_at" DESC);

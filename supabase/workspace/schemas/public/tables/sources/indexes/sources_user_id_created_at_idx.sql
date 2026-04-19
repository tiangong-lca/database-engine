CREATE INDEX "sources_user_id_created_at_idx" ON "public"."sources" USING "btree" ("user_id", "created_at" DESC);

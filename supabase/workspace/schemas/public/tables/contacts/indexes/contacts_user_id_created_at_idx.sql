CREATE INDEX "contacts_user_id_created_at_idx" ON "public"."contacts" USING "btree" ("user_id", "created_at" DESC);

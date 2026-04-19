CREATE INDEX "ilcd_user_id_created_at_idx" ON "public"."ilcd" USING "btree" ("user_id", "created_at" DESC);

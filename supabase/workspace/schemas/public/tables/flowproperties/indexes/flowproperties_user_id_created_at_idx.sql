CREATE INDEX "flowproperties_user_id_created_at_idx" ON "public"."flowproperties" USING "btree" ("user_id", "created_at" DESC);

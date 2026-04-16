CREATE INDEX "lifecyclemodels_user_id_created_at_idx" ON "public"."lifecyclemodels" USING "btree" ("user_id", "created_at" DESC);

CREATE INDEX "lciamethods_user_id_created_at_idx" ON "public"."lciamethods" USING "btree" ("user_id", "created_at" DESC);

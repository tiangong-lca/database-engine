CREATE INDEX "flows_public_latest_keys_cover_idx" ON "public"."flows" USING "btree" ("id", "version" DESC, "modified_at" DESC) INCLUDE ("created_at", "team_id") WHERE ("state_code" = 100);

CREATE INDEX "flows_portal_embedding_eligible_v1_idx" ON "public"."flows" USING "btree" ("state_code") WHERE (("state_code" = ANY (ARRAY[100, 200])) AND ("embedding_ft" IS NOT NULL));

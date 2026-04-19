CREATE INDEX "sources_json_idx" ON "public"."sources" USING "gin" ("json");

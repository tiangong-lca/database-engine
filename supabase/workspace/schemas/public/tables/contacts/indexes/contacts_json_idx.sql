CREATE INDEX "contacts_json_idx" ON "public"."contacts" USING "gin" ("json");

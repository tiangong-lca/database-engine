CREATE INDEX "contacts_text_pgroonga" ON "public"."contacts" USING "pgroonga" ("extracted_text");

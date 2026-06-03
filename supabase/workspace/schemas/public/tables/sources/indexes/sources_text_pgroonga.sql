CREATE INDEX "sources_text_pgroonga" ON "public"."sources" USING "pgroonga" ("extracted_text");

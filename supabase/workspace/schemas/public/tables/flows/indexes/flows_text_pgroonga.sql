CREATE INDEX "flows_text_pgroonga" ON "public"."flows" USING "pgroonga" ("extracted_text");

CREATE INDEX "contacts_json_ordered_vector" ON "public"."contacts" USING "hnsw" ("embedding" "extensions"."vector_cosine_ops");

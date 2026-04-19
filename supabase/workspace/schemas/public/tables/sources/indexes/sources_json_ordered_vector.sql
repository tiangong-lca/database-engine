CREATE INDEX "sources_json_ordered_vector" ON "public"."sources" USING "hnsw" ("embedding" "extensions"."vector_cosine_ops");

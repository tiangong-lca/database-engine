CREATE INDEX "flowproperties_json_ordered_vector" ON "public"."flowproperties" USING "hnsw" ("embedding" "extensions"."vector_cosine_ops");

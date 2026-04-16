CREATE INDEX "unitgroups_json_ordered_vector" ON "public"."unitgroups" USING "hnsw" ("embedding" "extensions"."vector_cosine_ops");

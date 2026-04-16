CREATE INDEX "flows_embedding_ft_hnsw_idx" ON "public"."flows" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");

CREATE INDEX "processes_embedding_ft_hnsw_idx" ON "public"."processes" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");

CREATE INDEX "lifecyclemodels_embedding_ft_hnsw_idx" ON "public"."lifecyclemodels" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");

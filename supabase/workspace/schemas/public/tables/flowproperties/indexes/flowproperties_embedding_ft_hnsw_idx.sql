CREATE INDEX "flowproperties_embedding_ft_hnsw_idx" ON "public"."flowproperties" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops");

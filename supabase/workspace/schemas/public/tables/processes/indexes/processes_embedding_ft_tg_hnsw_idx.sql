CREATE INDEX "processes_embedding_ft_tg_hnsw_idx" ON "public"."processes" USING "hnsw" ("embedding_ft" "extensions"."vector_cosine_ops") WHERE (("state_code" = 100) AND ("embedding_ft" IS NOT NULL));

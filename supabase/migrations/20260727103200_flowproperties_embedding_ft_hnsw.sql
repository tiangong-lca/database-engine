create index concurrently if not exists flowproperties_embedding_ft_hnsw_idx
  on public.flowproperties using hnsw (embedding_ft extensions.vector_cosine_ops);

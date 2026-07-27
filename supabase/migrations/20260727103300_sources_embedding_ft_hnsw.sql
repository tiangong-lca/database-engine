create index concurrently if not exists sources_embedding_ft_hnsw_idx
  on public.sources using hnsw (embedding_ft extensions.vector_cosine_ops);

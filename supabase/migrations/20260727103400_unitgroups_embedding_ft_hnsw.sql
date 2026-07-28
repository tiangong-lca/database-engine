create index concurrently if not exists unitgroups_embedding_ft_hnsw_idx
  on public.unitgroups using hnsw (embedding_ft extensions.vector_cosine_ops);

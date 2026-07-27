create index concurrently if not exists contacts_embedding_ft_hnsw_idx
  on public.contacts using hnsw (embedding_ft extensions.vector_cosine_ops);

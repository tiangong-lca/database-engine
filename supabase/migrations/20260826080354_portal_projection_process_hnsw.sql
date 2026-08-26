create index concurrently portal_catalog_search_process_embedding_v1_hnsw
  on private.portal_catalog_search_rows_v1 using hnsw (
    embedding_ft extensions.vector_cosine_ops
  )
  where dataset_kind = 'process'
    and embedding_ft is not null;

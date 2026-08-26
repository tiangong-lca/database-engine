create index concurrently portal_catalog_search_flow_embedding_v1_hnsw
  on private.portal_catalog_search_rows_v1 using hnsw (
    embedding_ft extensions.vector_cosine_ops
  )
  where dataset_kind = 'flow'
    and embedding_ft is not null;

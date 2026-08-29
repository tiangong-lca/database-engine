create index concurrently if not exists lca_package_request_cache_gc_candidate_idx
  on private.lca_package_request_cache (last_accessed_at, created_at, id)
  where status not in ('pending', 'running');

create index concurrently if not exists lca_result_cache_snapshot_idx
  on public.lca_result_cache (snapshot_id);

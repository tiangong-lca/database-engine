create index concurrently if not exists lca_package_artifacts_gc_candidate_idx
  on private.lca_package_artifacts (expires_at, created_at, id)
  where expires_at is not null
    and is_pinned is false
    and status = 'ready';

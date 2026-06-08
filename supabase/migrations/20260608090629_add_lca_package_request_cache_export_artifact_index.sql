create index concurrently if not exists lca_package_request_cache_export_artifact_idx
  on public.lca_package_request_cache (export_artifact_id)
  where export_artifact_id is not null;

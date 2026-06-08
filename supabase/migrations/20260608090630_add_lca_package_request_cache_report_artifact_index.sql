create index concurrently if not exists lca_package_request_cache_report_artifact_idx
  on public.lca_package_request_cache (report_artifact_id)
  where report_artifact_id is not null;

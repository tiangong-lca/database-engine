create index concurrently if not exists lca_package_export_items_gc_candidate_idx
  on private.lca_package_export_items (created_at, id)
  include (worker_job_id, job_id);

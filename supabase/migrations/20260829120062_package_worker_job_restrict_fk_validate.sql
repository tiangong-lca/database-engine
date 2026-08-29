set lock_timeout = '5s';
set statement_timeout = '15min';

alter table private.lca_package_artifacts
  validate constraint lca_package_artifacts_worker_job_id_restrict_fkey;
alter table private.lca_package_export_items
  validate constraint lca_package_export_items_worker_job_id_restrict_fkey;
alter table private.lca_package_request_cache
  validate constraint lca_package_request_cache_worker_job_id_restrict_fkey;

reset statement_timeout;
reset lock_timeout;

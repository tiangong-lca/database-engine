begin;
set local lock_timeout = '5s';
set local statement_timeout = '30s';

-- The validated scans ran in the prior migration. This transaction performs
-- only the short catalog swap and rolls back all three tables together if any
-- ACCESS EXCLUSIVE lock cannot be acquired within the lock budget.
do $$
begin
  if exists (
    select 1 from pg_catalog.pg_constraint
    where conname = 'lca_package_artifacts_worker_job_id_restrict_fkey'
      and conrelid = 'private.lca_package_artifacts'::regclass
  ) then
    alter table private.lca_package_artifacts
      drop constraint lca_package_artifacts_worker_job_id_fkey;
    alter table private.lca_package_artifacts
      rename constraint lca_package_artifacts_worker_job_id_restrict_fkey
      to lca_package_artifacts_worker_job_id_fkey;
  elsif not exists (
    select 1 from pg_catalog.pg_constraint
    where conname = 'lca_package_artifacts_worker_job_id_fkey'
      and conrelid = 'private.lca_package_artifacts'::regclass
      and confdeltype = 'r'
  ) then
    raise exception 'lca_package_artifacts restrict FK swap is not recoverable';
  end if;

  if exists (
    select 1 from pg_catalog.pg_constraint
    where conname = 'lca_package_export_items_worker_job_id_restrict_fkey'
      and conrelid = 'private.lca_package_export_items'::regclass
  ) then
    alter table private.lca_package_export_items
      drop constraint lca_package_export_items_worker_job_id_fkey;
    alter table private.lca_package_export_items
      rename constraint lca_package_export_items_worker_job_id_restrict_fkey
      to lca_package_export_items_worker_job_id_fkey;
  elsif not exists (
    select 1 from pg_catalog.pg_constraint
    where conname = 'lca_package_export_items_worker_job_id_fkey'
      and conrelid = 'private.lca_package_export_items'::regclass
      and confdeltype = 'r'
  ) then
    raise exception 'lca_package_export_items restrict FK swap is not recoverable';
  end if;

  if exists (
    select 1 from pg_catalog.pg_constraint
    where conname = 'lca_package_request_cache_worker_job_id_restrict_fkey'
      and conrelid = 'private.lca_package_request_cache'::regclass
  ) then
    alter table private.lca_package_request_cache
      drop constraint lca_package_request_cache_worker_job_id_fkey;
    alter table private.lca_package_request_cache
      rename constraint lca_package_request_cache_worker_job_id_restrict_fkey
      to lca_package_request_cache_worker_job_id_fkey;
  elsif not exists (
    select 1 from pg_catalog.pg_constraint
    where conname = 'lca_package_request_cache_worker_job_id_fkey'
      and conrelid = 'private.lca_package_request_cache'::regclass
      and confdeltype = 'r'
  ) then
    raise exception 'lca_package_request_cache restrict FK swap is not recoverable';
  end if;
end;
$$;

commit;

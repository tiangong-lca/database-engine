set lock_timeout = '5s';
set statement_timeout = '30s';

do $$
begin
  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conname = 'lca_package_artifacts_worker_job_id_restrict_fkey'
      and conrelid = 'private.lca_package_artifacts'::regclass
  ) then
    alter table private.lca_package_artifacts
      add constraint lca_package_artifacts_worker_job_id_restrict_fkey
      foreign key (worker_job_id) references private.worker_jobs(id)
      on delete restrict not valid;
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conname = 'lca_package_export_items_worker_job_id_restrict_fkey'
      and conrelid = 'private.lca_package_export_items'::regclass
  ) then
    alter table private.lca_package_export_items
      add constraint lca_package_export_items_worker_job_id_restrict_fkey
      foreign key (worker_job_id) references private.worker_jobs(id)
      on delete restrict not valid;
  end if;
end;
$$;

do $$
begin
  if not exists (
    select 1 from pg_catalog.pg_constraint
    where conname = 'lca_package_request_cache_worker_job_id_restrict_fkey'
      and conrelid = 'private.lca_package_request_cache'::regclass
  ) then
    alter table private.lca_package_request_cache
      add constraint lca_package_request_cache_worker_job_id_restrict_fkey
      foreign key (worker_job_id) references private.worker_jobs(id)
      on delete restrict not valid;
  end if;
end;
$$;

reset statement_timeout;
reset lock_timeout;

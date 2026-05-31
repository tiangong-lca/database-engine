create schema if not exists util;

create table if not exists public.lca_snapshot_gc_runs (
  id uuid primary key default gen_random_uuid(),
  mode text not null,
  status text not null default 'running',
  started_at timestamp with time zone not null default now(),
  finished_at timestamp with time zone,
  as_of timestamp with time zone not null default now(),
  snapshot_retention_window interval not null default interval '30 days',
  orphan_retention_window interval not null default interval '30 days',
  max_snapshots integer not null default 100,
  max_orphan_dirs integer not null default 200,
  max_bytes bigint not null default 2147483648,
  candidate_snapshot_count integer not null default 0,
  candidate_orphan_dir_count integer not null default 0,
  candidate_object_count integer not null default 0,
  candidate_storage_bytes bigint not null default 0,
  storage_deleted_count integer not null default 0,
  storage_failed_count integer not null default 0,
  db_snapshot_deleted_count integer not null default 0,
  diagnostics jsonb not null default '{}'::jsonb,
  created_at timestamp with time zone not null default now(),
  constraint lca_snapshot_gc_runs_mode_chk
    check (mode = any (array['dry_run'::text, 'execute'::text])),
  constraint lca_snapshot_gc_runs_status_chk
    check (status = any (array['running'::text, 'succeeded'::text, 'failed'::text, 'skipped'::text])),
  constraint lca_snapshot_gc_runs_windows_chk
    check (
      snapshot_retention_window >= interval '1 day'
      and orphan_retention_window >= interval '1 day'
    ),
  constraint lca_snapshot_gc_runs_caps_chk
    check (
      max_snapshots > 0
      and max_orphan_dirs > 0
      and max_bytes > 0
    ),
  constraint lca_snapshot_gc_runs_counts_chk
    check (
      candidate_snapshot_count >= 0
      and candidate_orphan_dir_count >= 0
      and candidate_object_count >= 0
      and candidate_storage_bytes >= 0
      and storage_deleted_count >= 0
      and storage_failed_count >= 0
      and db_snapshot_deleted_count >= 0
    )
);

alter table public.lca_snapshot_gc_runs owner to postgres;

create index if not exists lca_snapshot_gc_runs_started_idx
  on public.lca_snapshot_gc_runs (started_at desc);

create table if not exists public.lca_snapshot_gc_run_items (
  id uuid primary key default gen_random_uuid(),
  run_id uuid not null references public.lca_snapshot_gc_runs(id) on delete cascade,
  candidate_type text not null,
  snapshot_id uuid,
  bucket_id text not null,
  object_name text not null,
  storage_bytes bigint not null default 0,
  reason text not null,
  delete_db_snapshot boolean not null default false,
  action_status text not null default 'planned',
  error_message text,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),
  constraint lca_snapshot_gc_run_items_candidate_type_chk
    check (candidate_type = any (array['snapshot_directory'::text, 'orphan_storage_directory'::text])),
  constraint lca_snapshot_gc_run_items_action_status_chk
    check (action_status = any (
      array[
        'planned'::text,
        'dry_run'::text,
        'storage_deleted'::text,
        'storage_missing'::text,
        'storage_failed'::text,
        'db_deleted'::text,
        'skipped'::text
      ]
    )),
  constraint lca_snapshot_gc_run_items_storage_bytes_chk
    check (storage_bytes >= 0)
);

alter table public.lca_snapshot_gc_run_items owner to postgres;

create index if not exists lca_snapshot_gc_run_items_run_idx
  on public.lca_snapshot_gc_run_items (run_id);

create index if not exists lca_snapshot_gc_run_items_snapshot_idx
  on public.lca_snapshot_gc_run_items (snapshot_id)
  where snapshot_id is not null;

create index if not exists lca_snapshot_gc_run_items_status_idx
  on public.lca_snapshot_gc_run_items (action_status, created_at desc);

alter table public.lca_snapshot_gc_runs enable row level security;
alter table public.lca_snapshot_gc_run_items enable row level security;

drop policy if exists lca_snapshot_gc_runs_service_role_all
  on public.lca_snapshot_gc_runs;
create policy lca_snapshot_gc_runs_service_role_all
  on public.lca_snapshot_gc_runs
  to service_role
  using (true)
  with check (true);

drop policy if exists lca_snapshot_gc_run_items_service_role_all
  on public.lca_snapshot_gc_run_items;
create policy lca_snapshot_gc_run_items_service_role_all
  on public.lca_snapshot_gc_run_items
  to service_role
  using (true)
  with check (true);

revoke all on table public.lca_snapshot_gc_runs from public;
revoke all on table public.lca_snapshot_gc_run_items from public;
grant all on table public.lca_snapshot_gc_runs to service_role;
grant all on table public.lca_snapshot_gc_run_items to service_role;

comment on table public.lca_snapshot_gc_runs is
  'Audit header for calculator-driven lca-results/snapshots object-aware garbage collection runs.';

comment on table public.lca_snapshot_gc_run_items is
  'Per-object audit items for calculator-driven lca-results/snapshots object-aware garbage collection runs.';

create or replace function util.preview_lca_snapshot_retention(
  p_snapshot_retention_window interval default interval '30 days',
  p_orphan_retention_window interval default interval '30 days',
  p_as_of timestamp with time zone default pg_catalog.now()
) returns table (
  retention_area text,
  retention_action text,
  is_eligible boolean,
  reason text,
  retention_window interval,
  cutoff_time timestamp with time zone,
  snapshot_count bigint,
  object_count bigint,
  total_storage_bytes bigint,
  downstream_active_count bigint,
  downstream_job_count bigint,
  downstream_result_count bigint,
  downstream_cache_count bigint,
  downstream_latest_count bigint,
  downstream_factorization_count bigint,
  downstream_artifact_count bigint,
  oldest_observed_at timestamp with time zone,
  newest_observed_at timestamp with time zone
)
language plpgsql
stable
set search_path to ''
as $$
begin
  if p_as_of is null then
    raise exception using
      errcode = '22023',
      message = 'snapshot retention preview as_of timestamp must not be null';
  end if;

  if p_snapshot_retention_window < interval '1 day' then
    raise exception using
      errcode = '22023',
      message = 'snapshot retention window must be at least 1 day';
  end if;

  if p_orphan_retention_window < interval '1 day' then
    raise exception using
      errcode = '22023',
      message = 'orphan snapshot storage retention window must be at least 1 day';
  end if;

  return query
  with storage_objects as (
    select
      objects.bucket_id,
      objects.name as object_name,
      case
        when objects.name ~ '^lca-results/snapshots/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/'
          then substring(objects.name from '^lca-results/snapshots/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})/')::uuid
        else null::uuid
      end as parsed_snapshot_id,
      nullif(split_part(objects.name, '/', 3), '') as snapshot_directory,
      coalesce(
        case
          when jsonb_typeof(objects.metadata -> 'size') = 'number'
            or coalesce(objects.metadata ->> 'size', '') ~ '^[0-9]+$'
            then (objects.metadata ->> 'size')::bigint
          else null::bigint
        end,
        0
      ) as storage_bytes,
      coalesce(objects.created_at, objects.updated_at) as object_created_at,
      coalesce(objects.updated_at, objects.created_at) as object_updated_at
    from storage.objects as objects
    where objects.bucket_id = 'lca_results'
      and objects.name like 'lca-results/snapshots/%'
  ),
  storage_directories as (
    select
      storage_objects.bucket_id,
      storage_objects.parsed_snapshot_id,
      storage_objects.snapshot_directory,
      count(*)::bigint as object_count,
      coalesce(sum(storage_objects.storage_bytes), 0)::bigint as total_storage_bytes,
      min(coalesce(storage_objects.object_created_at, storage_objects.object_updated_at)) as oldest_object_at,
      max(coalesce(storage_objects.object_created_at, storage_objects.object_updated_at)) as newest_object_at
    from storage_objects
    group by
      storage_objects.bucket_id,
      storage_objects.parsed_snapshot_id,
      storage_objects.snapshot_directory
  ),
  active_refs as (
    select lca_active_snapshots.snapshot_id, count(*)::bigint as active_ref_count
    from public.lca_active_snapshots
    group by lca_active_snapshots.snapshot_id
  ),
  job_refs as (
    select lca_jobs.snapshot_id, count(*)::bigint as job_ref_count
    from public.lca_jobs
    group by lca_jobs.snapshot_id
  ),
  result_refs as (
    select lca_results.snapshot_id, count(*)::bigint as result_ref_count
    from public.lca_results
    group by lca_results.snapshot_id
  ),
  cache_refs as (
    select lca_result_cache.snapshot_id, count(*)::bigint as cache_ref_count
    from public.lca_result_cache
    group by lca_result_cache.snapshot_id
  ),
  latest_refs as (
    select lca_latest_all_unit_results.snapshot_id, count(*)::bigint as latest_ref_count
    from public.lca_latest_all_unit_results
    group by lca_latest_all_unit_results.snapshot_id
  ),
  factorization_refs as (
    select lca_factorization_registry.snapshot_id, count(*)::bigint as factorization_ref_count
    from public.lca_factorization_registry
    group by lca_factorization_registry.snapshot_id
  ),
  artifact_refs as (
    select lca_snapshot_artifacts.snapshot_id, count(*)::bigint as artifact_ref_count
    from public.lca_snapshot_artifacts
    group by lca_snapshot_artifacts.snapshot_id
  ),
  classified as (
    select
      case
        when storage_directories.parsed_snapshot_id is null then 'lca_snapshot_storage_unparsed_paths'
        when snapshots.id is null then 'lca_snapshot_storage_orphan_directories'
        else 'lca_snapshot_storage_directories'
      end as retention_area,
      case
        when storage_directories.parsed_snapshot_id is null then 'report_only'
        when snapshots.id is null then 'delete_storage_objects_only'
        else 'delete_storage_objects_then_snapshot_row'
      end as retention_action,
      case
        when storage_directories.parsed_snapshot_id is null then 'protected_unparsed_storage_path'
        when snapshots.id is null
          and storage_directories.newest_object_at >= p_as_of - p_orphan_retention_window
          then 'protected_inside_retention_window'
        when snapshots.id is null then 'eligible_orphan_storage_directory'
        when coalesce(active_refs.active_ref_count, 0) > 0 then 'protected_active_snapshot'
        when snapshot_ttl.expires_at_utc is not null
          and snapshot_ttl.expires_at_utc > p_as_of
          then 'protected_ttl_future'
        when snapshot_ttl.expires_at_utc is not null then 'eligible_ttl_expired_snapshot'
        when coalesce(snapshots.updated_at, snapshots.created_at) >= p_as_of - p_snapshot_retention_window
          then 'protected_inside_retention_window'
        else 'eligible_default_30d_snapshot'
      end as reason,
      case
        when snapshots.id is null then p_orphan_retention_window
        when snapshot_ttl.expires_at_utc is not null then null::interval
        else p_snapshot_retention_window
      end as retention_window,
      case
        when snapshots.id is null then p_as_of - p_orphan_retention_window
        when snapshot_ttl.expires_at_utc is not null then p_as_of
        else p_as_of - p_snapshot_retention_window
      end as cutoff_time,
      1::bigint as snapshot_count,
      storage_directories.object_count,
      storage_directories.total_storage_bytes,
      coalesce(active_refs.active_ref_count, 0)::bigint as active_ref_count,
      coalesce(job_refs.job_ref_count, 0)::bigint as job_ref_count,
      coalesce(result_refs.result_ref_count, 0)::bigint as result_ref_count,
      coalesce(cache_refs.cache_ref_count, 0)::bigint as cache_ref_count,
      coalesce(latest_refs.latest_ref_count, 0)::bigint as latest_ref_count,
      coalesce(factorization_refs.factorization_ref_count, 0)::bigint as factorization_ref_count,
      coalesce(artifact_refs.artifact_ref_count, 0)::bigint as artifact_ref_count,
      coalesce(snapshots.updated_at, snapshots.created_at, storage_directories.oldest_object_at) as oldest_observed_at,
      coalesce(snapshots.updated_at, snapshots.created_at, storage_directories.newest_object_at) as newest_observed_at,
      (
        case
          when storage_directories.parsed_snapshot_id is null then 'protected_unparsed_storage_path'
          when snapshots.id is null
            and storage_directories.newest_object_at >= p_as_of - p_orphan_retention_window
            then 'protected_inside_retention_window'
          when snapshots.id is null then 'eligible_orphan_storage_directory'
          when coalesce(active_refs.active_ref_count, 0) > 0 then 'protected_active_snapshot'
          when snapshot_ttl.expires_at_utc is not null
            and snapshot_ttl.expires_at_utc > p_as_of
            then 'protected_ttl_future'
          when snapshot_ttl.expires_at_utc is not null then 'eligible_ttl_expired_snapshot'
          when coalesce(snapshots.updated_at, snapshots.created_at) >= p_as_of - p_snapshot_retention_window
            then 'protected_inside_retention_window'
          else 'eligible_default_30d_snapshot'
        end
      ) in (
        'eligible_ttl_expired_snapshot',
        'eligible_default_30d_snapshot',
        'eligible_orphan_storage_directory'
      ) as is_eligible
    from storage_directories
    left join public.lca_network_snapshots as snapshots
      on snapshots.id = storage_directories.parsed_snapshot_id
    left join lateral (
      select case
        when coalesce(snapshots.process_filter #>> '{artifact_lifecycle,expires_at_utc}', '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T]'
          then (snapshots.process_filter #>> '{artifact_lifecycle,expires_at_utc}')::timestamp with time zone
        else null::timestamp with time zone
      end as expires_at_utc
    ) as snapshot_ttl on true
    left join active_refs
      on active_refs.snapshot_id = snapshots.id
    left join job_refs
      on job_refs.snapshot_id = snapshots.id
    left join result_refs
      on result_refs.snapshot_id = snapshots.id
    left join cache_refs
      on cache_refs.snapshot_id = snapshots.id
    left join latest_refs
      on latest_refs.snapshot_id = snapshots.id
    left join factorization_refs
      on factorization_refs.snapshot_id = snapshots.id
    left join artifact_refs
      on artifact_refs.snapshot_id = snapshots.id
  )
  select
    classified.retention_area,
    classified.retention_action,
    classified.is_eligible,
    classified.reason,
    classified.retention_window,
    classified.cutoff_time,
    sum(classified.snapshot_count)::bigint as snapshot_count,
    sum(classified.object_count)::bigint as object_count,
    coalesce(sum(classified.total_storage_bytes), 0)::bigint as total_storage_bytes,
    coalesce(sum(classified.active_ref_count), 0)::bigint as downstream_active_count,
    coalesce(sum(classified.job_ref_count), 0)::bigint as downstream_job_count,
    coalesce(sum(classified.result_ref_count), 0)::bigint as downstream_result_count,
    coalesce(sum(classified.cache_ref_count), 0)::bigint as downstream_cache_count,
    coalesce(sum(classified.latest_ref_count), 0)::bigint as downstream_latest_count,
    coalesce(sum(classified.factorization_ref_count), 0)::bigint as downstream_factorization_count,
    coalesce(sum(classified.artifact_ref_count), 0)::bigint as downstream_artifact_count,
    min(classified.oldest_observed_at) as oldest_observed_at,
    max(classified.newest_observed_at) as newest_observed_at
  from classified
  group by
    classified.retention_area,
    classified.retention_action,
    classified.is_eligible,
    classified.reason,
    classified.retention_window,
    classified.cutoff_time
  order by
    classified.retention_area,
    classified.is_eligible desc,
    classified.reason;
end;
$$;

alter function util.preview_lca_snapshot_retention(interval, interval, timestamp with time zone) owner to postgres;
revoke all on function util.preview_lca_snapshot_retention(interval, interval, timestamp with time zone) from public;
grant usage on schema util to service_role;
grant execute on function util.preview_lca_snapshot_retention(interval, interval, timestamp with time zone) to service_role;

comment on function util.preview_lca_snapshot_retention(interval, interval, timestamp with time zone) is
  'Operator dry-run helper for lca-results/snapshots retention; reports eligible/protected storage directory counts and downstream references without deleting storage objects or database rows.';

create or replace function util.list_lca_snapshot_gc_candidates(
  p_snapshot_retention_window interval default interval '30 days',
  p_orphan_retention_window interval default interval '30 days',
  p_as_of timestamp with time zone default pg_catalog.now(),
  p_max_snapshots integer default 100,
  p_max_orphan_dirs integer default 200,
  p_max_bytes bigint default 2147483648
) returns table (
  candidate_type text,
  snapshot_id uuid,
  snapshot_directory text,
  bucket_id text,
  object_name text,
  storage_bytes bigint,
  reason text,
  delete_db_snapshot boolean,
  snapshot_status text,
  snapshot_created_at timestamp with time zone,
  snapshot_updated_at timestamp with time zone,
  effective_expires_at timestamp with time zone,
  object_count bigint,
  snapshot_storage_bytes bigint,
  downstream_active_count bigint,
  downstream_job_count bigint,
  downstream_result_count bigint,
  downstream_cache_count bigint,
  downstream_latest_count bigint,
  downstream_factorization_count bigint,
  downstream_artifact_count bigint
)
language plpgsql
stable
set search_path to ''
as $$
begin
  if p_as_of is null then
    raise exception using
      errcode = '22023',
      message = 'snapshot GC candidate as_of timestamp must not be null';
  end if;

  if p_snapshot_retention_window < interval '1 day' then
    raise exception using
      errcode = '22023',
      message = 'snapshot retention window must be at least 1 day';
  end if;

  if p_orphan_retention_window < interval '1 day' then
    raise exception using
      errcode = '22023',
      message = 'orphan snapshot storage retention window must be at least 1 day';
  end if;

  if p_max_snapshots <= 0 then
    raise exception using
      errcode = '22023',
      message = 'snapshot GC max_snapshots must be greater than zero';
  end if;

  if p_max_orphan_dirs <= 0 then
    raise exception using
      errcode = '22023',
      message = 'snapshot GC max_orphan_dirs must be greater than zero';
  end if;

  if p_max_bytes <= 0 then
    raise exception using
      errcode = '22023',
      message = 'snapshot GC max_bytes must be greater than zero';
  end if;

  return query
  with storage_objects as (
    select
      objects.bucket_id,
      objects.name as object_name,
      case
        when objects.name ~ '^lca-results/snapshots/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/'
          then substring(objects.name from '^lca-results/snapshots/([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})/')::uuid
        else null::uuid
      end as parsed_snapshot_id,
      nullif(split_part(objects.name, '/', 3), '') as snapshot_directory,
      coalesce(
        case
          when jsonb_typeof(objects.metadata -> 'size') = 'number'
            or coalesce(objects.metadata ->> 'size', '') ~ '^[0-9]+$'
            then (objects.metadata ->> 'size')::bigint
          else null::bigint
        end,
        0
      ) as storage_bytes,
      coalesce(objects.created_at, objects.updated_at) as object_created_at,
      coalesce(objects.updated_at, objects.created_at) as object_updated_at
    from storage.objects as objects
    where objects.bucket_id = 'lca_results'
      and objects.name like 'lca-results/snapshots/%'
  ),
  storage_directories as (
    select
      storage_objects.bucket_id,
      storage_objects.parsed_snapshot_id,
      storage_objects.snapshot_directory,
      count(*)::bigint as object_count,
      coalesce(sum(storage_objects.storage_bytes), 0)::bigint as total_storage_bytes,
      min(coalesce(storage_objects.object_created_at, storage_objects.object_updated_at)) as oldest_object_at,
      max(coalesce(storage_objects.object_created_at, storage_objects.object_updated_at)) as newest_object_at
    from storage_objects
    group by
      storage_objects.bucket_id,
      storage_objects.parsed_snapshot_id,
      storage_objects.snapshot_directory
  ),
  active_refs as (
    select lca_active_snapshots.snapshot_id, count(*)::bigint as active_ref_count
    from public.lca_active_snapshots
    group by lca_active_snapshots.snapshot_id
  ),
  job_refs as (
    select lca_jobs.snapshot_id, count(*)::bigint as job_ref_count
    from public.lca_jobs
    group by lca_jobs.snapshot_id
  ),
  result_refs as (
    select lca_results.snapshot_id, count(*)::bigint as result_ref_count
    from public.lca_results
    group by lca_results.snapshot_id
  ),
  cache_refs as (
    select lca_result_cache.snapshot_id, count(*)::bigint as cache_ref_count
    from public.lca_result_cache
    group by lca_result_cache.snapshot_id
  ),
  latest_refs as (
    select lca_latest_all_unit_results.snapshot_id, count(*)::bigint as latest_ref_count
    from public.lca_latest_all_unit_results
    group by lca_latest_all_unit_results.snapshot_id
  ),
  factorization_refs as (
    select lca_factorization_registry.snapshot_id, count(*)::bigint as factorization_ref_count
    from public.lca_factorization_registry
    group by lca_factorization_registry.snapshot_id
  ),
  artifact_refs as (
    select lca_snapshot_artifacts.snapshot_id, count(*)::bigint as artifact_ref_count
    from public.lca_snapshot_artifacts
    group by lca_snapshot_artifacts.snapshot_id
  ),
  classified as (
    select
      case
        when snapshots.id is null then 'orphan_storage_directory'
        else 'snapshot_directory'
      end as candidate_type,
      storage_directories.parsed_snapshot_id as snapshot_id,
      storage_directories.snapshot_directory,
      storage_directories.bucket_id,
      snapshots.status as snapshot_status,
      snapshots.created_at as snapshot_created_at,
      snapshots.updated_at as snapshot_updated_at,
      case
        when snapshot_ttl.expires_at_utc is not null then snapshot_ttl.expires_at_utc
        when snapshots.id is not null then coalesce(snapshots.updated_at, snapshots.created_at) + p_snapshot_retention_window
        else storage_directories.newest_object_at + p_orphan_retention_window
      end as effective_expires_at,
      storage_directories.object_count,
      storage_directories.total_storage_bytes,
      coalesce(active_refs.active_ref_count, 0)::bigint as active_ref_count,
      coalesce(job_refs.job_ref_count, 0)::bigint as job_ref_count,
      coalesce(result_refs.result_ref_count, 0)::bigint as result_ref_count,
      coalesce(cache_refs.cache_ref_count, 0)::bigint as cache_ref_count,
      coalesce(latest_refs.latest_ref_count, 0)::bigint as latest_ref_count,
      coalesce(factorization_refs.factorization_ref_count, 0)::bigint as factorization_ref_count,
      coalesce(artifact_refs.artifact_ref_count, 0)::bigint as artifact_ref_count,
      coalesce(snapshots.updated_at, snapshots.created_at, storage_directories.newest_object_at) as observed_at,
      case
        when storage_directories.parsed_snapshot_id is null then 'protected_unparsed_storage_path'
        when snapshots.id is null
          and storage_directories.newest_object_at >= p_as_of - p_orphan_retention_window
          then 'protected_inside_retention_window'
        when snapshots.id is null then 'eligible_orphan_storage_directory'
        when coalesce(active_refs.active_ref_count, 0) > 0 then 'protected_active_snapshot'
        when snapshot_ttl.expires_at_utc is not null
          and snapshot_ttl.expires_at_utc > p_as_of
          then 'protected_ttl_future'
        when snapshot_ttl.expires_at_utc is not null then 'eligible_ttl_expired_snapshot'
        when coalesce(snapshots.updated_at, snapshots.created_at) >= p_as_of - p_snapshot_retention_window
          then 'protected_inside_retention_window'
        else 'eligible_default_30d_snapshot'
      end as reason
    from storage_directories
    left join public.lca_network_snapshots as snapshots
      on snapshots.id = storage_directories.parsed_snapshot_id
    left join lateral (
      select case
        when coalesce(snapshots.process_filter #>> '{artifact_lifecycle,expires_at_utc}', '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T]'
          then (snapshots.process_filter #>> '{artifact_lifecycle,expires_at_utc}')::timestamp with time zone
        else null::timestamp with time zone
      end as expires_at_utc
    ) as snapshot_ttl on true
    left join active_refs
      on active_refs.snapshot_id = snapshots.id
    left join job_refs
      on job_refs.snapshot_id = snapshots.id
    left join result_refs
      on result_refs.snapshot_id = snapshots.id
    left join cache_refs
      on cache_refs.snapshot_id = snapshots.id
    left join latest_refs
      on latest_refs.snapshot_id = snapshots.id
    left join factorization_refs
      on factorization_refs.snapshot_id = snapshots.id
    left join artifact_refs
      on artifact_refs.snapshot_id = snapshots.id
  ),
  eligible as (
    select
      classified.*,
      row_number() over (
        partition by classified.candidate_type
        order by classified.effective_expires_at, classified.snapshot_directory
      ) as candidate_type_rank
    from classified
    where classified.reason in (
      'eligible_ttl_expired_snapshot',
      'eligible_default_30d_snapshot',
      'eligible_orphan_storage_directory'
    )
  ),
  type_capped as (
    select eligible.*
    from eligible
    where (
        eligible.candidate_type = 'snapshot_directory'
        and eligible.candidate_type_rank <= p_max_snapshots
      )
      or (
        eligible.candidate_type = 'orphan_storage_directory'
        and eligible.candidate_type_rank <= p_max_orphan_dirs
      )
  ),
  byte_capped as (
    select
      type_capped.*,
      sum(type_capped.total_storage_bytes) over (
        order by type_capped.effective_expires_at, type_capped.snapshot_directory
      ) as cumulative_storage_bytes
    from type_capped
  ),
  selected_directories as (
    select *
    from byte_capped
    where byte_capped.cumulative_storage_bytes <= p_max_bytes
  )
  select
    selected_directories.candidate_type,
    selected_directories.snapshot_id,
    selected_directories.snapshot_directory,
    storage_objects.bucket_id,
    storage_objects.object_name,
    storage_objects.storage_bytes,
    selected_directories.reason,
    (selected_directories.candidate_type = 'snapshot_directory') as delete_db_snapshot,
    selected_directories.snapshot_status,
    selected_directories.snapshot_created_at,
    selected_directories.snapshot_updated_at,
    selected_directories.effective_expires_at,
    selected_directories.object_count,
    selected_directories.total_storage_bytes as snapshot_storage_bytes,
    selected_directories.active_ref_count as downstream_active_count,
    selected_directories.job_ref_count as downstream_job_count,
    selected_directories.result_ref_count as downstream_result_count,
    selected_directories.cache_ref_count as downstream_cache_count,
    selected_directories.latest_ref_count as downstream_latest_count,
    selected_directories.factorization_ref_count as downstream_factorization_count,
    selected_directories.artifact_ref_count as downstream_artifact_count
  from selected_directories
  join storage_objects
    on storage_objects.bucket_id = selected_directories.bucket_id
   and storage_objects.snapshot_directory = selected_directories.snapshot_directory
   and (
      storage_objects.parsed_snapshot_id = selected_directories.snapshot_id
      or (
        storage_objects.parsed_snapshot_id is null
        and selected_directories.snapshot_id is null
      )
   )
  order by
    selected_directories.effective_expires_at,
    selected_directories.snapshot_directory,
    storage_objects.object_name;
end;
$$;

alter function util.list_lca_snapshot_gc_candidates(
  interval,
  interval,
  timestamp with time zone,
  integer,
  integer,
  bigint
) owner to postgres;
revoke all on function util.list_lca_snapshot_gc_candidates(
  interval,
  interval,
  timestamp with time zone,
  integer,
  integer,
  bigint
) from public;
grant execute on function util.list_lca_snapshot_gc_candidates(
  interval,
  interval,
  timestamp with time zone,
  integer,
  integer,
  bigint
) to service_role;

comment on function util.list_lca_snapshot_gc_candidates(
  interval,
  interval,
  timestamp with time zone,
  integer,
  integer,
  bigint
) is
  'Object-level candidate contract for calculator-driven lca-results/snapshots GC; applies snapshot/orphan/byte caps but never deletes storage objects or database rows.';

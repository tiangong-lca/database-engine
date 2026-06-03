create schema if not exists archive;

revoke all on schema archive from public, anon, authenticated;
grant usage on schema archive to service_role;

create table if not exists archive.worker_legacy_job_table_rows (
  archive_id uuid primary key default gen_random_uuid(),
  archived_at timestamptz not null default now(),
  source_table text not null,
  source_row_id uuid,
  source_created_at timestamptz,
  source_modified_at timestamptz,
  archive_reason text not null default 'worker_jobs_cutover_legacy_table_retirement',
  row_payload jsonb not null,
  constraint worker_legacy_job_table_rows_source_check
    check (source_table in (
      'public.lca_jobs',
      'public.lca_package_jobs',
      'public.dataset_review_submit_jobs'
    )),
  constraint worker_legacy_job_table_rows_payload_check
    check (jsonb_typeof(row_payload) = 'object')
);

alter table archive.worker_legacy_job_table_rows enable row level security;

create unique index if not exists worker_legacy_job_table_rows_source_uidx
  on archive.worker_legacy_job_table_rows (source_table, source_row_id)
  where source_row_id is not null;

revoke all on archive.worker_legacy_job_table_rows from public, anon, authenticated;
grant select, insert, update on archive.worker_legacy_job_table_rows to service_role;

comment on table archive.worker_legacy_job_table_rows
  is 'Archive artifact for retired worker legacy job tables before physical DROP TABLE. Restore path: recreate the retired table from historical migrations, then insert rows by expanding row_payload for the relevant source_table.';
comment on column archive.worker_legacy_job_table_rows.row_payload
  is 'Full to_jsonb(row) payload captured before public.lca_jobs, public.lca_package_jobs, and public.dataset_review_submit_jobs were dropped.';

do $$
begin
  if to_regclass('public.lca_jobs') is not null then
    execute $sql$
      insert into archive.worker_legacy_job_table_rows (
        source_table,
        source_row_id,
        source_created_at,
        source_modified_at,
        row_payload
      )
      select
        'public.lca_jobs',
        legacy_row.id,
        legacy_row.created_at,
        legacy_row.updated_at,
        to_jsonb(legacy_row)
      from public.lca_jobs as legacy_row
      on conflict (source_table, source_row_id) where source_row_id is not null
      do update
        set source_created_at = excluded.source_created_at,
            source_modified_at = excluded.source_modified_at,
            row_payload = excluded.row_payload,
            archived_at = now()
    $sql$;
  end if;

  if to_regclass('public.lca_package_jobs') is not null then
    execute $sql$
      insert into archive.worker_legacy_job_table_rows (
        source_table,
        source_row_id,
        source_created_at,
        source_modified_at,
        row_payload
      )
      select
        'public.lca_package_jobs',
        legacy_row.id,
        legacy_row.created_at,
        legacy_row.updated_at,
        to_jsonb(legacy_row)
      from public.lca_package_jobs as legacy_row
      on conflict (source_table, source_row_id) where source_row_id is not null
      do update
        set source_created_at = excluded.source_created_at,
            source_modified_at = excluded.source_modified_at,
            row_payload = excluded.row_payload,
            archived_at = now()
    $sql$;
  end if;

  if to_regclass('public.dataset_review_submit_jobs') is not null then
    execute $sql$
      insert into archive.worker_legacy_job_table_rows (
        source_table,
        source_row_id,
        source_created_at,
        source_modified_at,
        row_payload
      )
      select
        'public.dataset_review_submit_jobs',
        legacy_row.id,
        legacy_row.created_at,
        legacy_row.modified_at,
        to_jsonb(legacy_row)
      from public.dataset_review_submit_jobs as legacy_row
      on conflict (source_table, source_row_id) where source_row_id is not null
      do update
        set source_created_at = excluded.source_created_at,
            source_modified_at = excluded.source_modified_at,
            row_payload = excluded.row_payload,
            archived_at = now()
    $sql$;
  end if;
end
$$;

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
  worker_job_ref_candidates as (
    select distinct
      jobs.id as worker_job_id,
      case
        when refs.snapshot_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then refs.snapshot_text::uuid
        else null::uuid
      end as snapshot_id
    from public.worker_jobs as jobs
    cross join lateral (
      values
        (jobs.payload_json #>> '{snapshot,id}'),
        (jobs.payload_json #>> '{snapshotId}'),
        (jobs.payload_json #>> '{snapshot_id}'),
        (jobs.result_json #>> '{snapshot,id}'),
        (jobs.result_json #>> '{snapshotId}'),
        (jobs.result_json #>> '{snapshot_id}')
    ) as refs(snapshot_text)
    where jobs.job_kind like 'lca.%'
  ),
  job_refs as (
    select
      worker_job_ref_candidates.snapshot_id,
      count(distinct worker_job_ref_candidates.worker_job_id)::bigint as job_ref_count
    from worker_job_ref_candidates
    where worker_job_ref_candidates.snapshot_id is not null
    group by worker_job_ref_candidates.snapshot_id
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
      coalesce(snapshots.updated_at, snapshots.created_at, storage_directories.newest_object_at) as newest_observed_at
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
    classified.reason in (
      'eligible_ttl_expired_snapshot',
      'eligible_default_30d_snapshot',
      'eligible_orphan_storage_directory'
    ) as is_eligible,
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
    classified.reason,
    classified.retention_window,
    classified.cutoff_time
  order by
    classified.retention_area,
    is_eligible desc,
    classified.reason;
end;
$$;

alter function util.preview_lca_snapshot_retention(interval, interval, timestamp with time zone) owner to postgres;
revoke all on function util.preview_lca_snapshot_retention(interval, interval, timestamp with time zone) from public;
grant usage on schema util to service_role;
grant execute on function util.preview_lca_snapshot_retention(interval, interval, timestamp with time zone) to service_role;

comment on function util.preview_lca_snapshot_retention(interval, interval, timestamp with time zone) is
  'Operator dry-run helper for lca-results/snapshots retention. Job references are counted from worker_jobs after public.lca_jobs retirement.';

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
  worker_job_ref_candidates as (
    select distinct
      jobs.id as worker_job_id,
      case
        when refs.snapshot_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          then refs.snapshot_text::uuid
        else null::uuid
      end as snapshot_id
    from public.worker_jobs as jobs
    cross join lateral (
      values
        (jobs.payload_json #>> '{snapshot,id}'),
        (jobs.payload_json #>> '{snapshotId}'),
        (jobs.payload_json #>> '{snapshot_id}'),
        (jobs.result_json #>> '{snapshot,id}'),
        (jobs.result_json #>> '{snapshotId}'),
        (jobs.result_json #>> '{snapshot_id}')
    ) as refs(snapshot_text)
    where jobs.job_kind like 'lca.%'
  ),
  job_refs as (
    select
      worker_job_ref_candidates.snapshot_id,
      count(distinct worker_job_ref_candidates.worker_job_id)::bigint as job_ref_count
    from worker_job_ref_candidates
    where worker_job_ref_candidates.snapshot_id is not null
    group by worker_job_ref_candidates.snapshot_id
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
      storage_directories.bucket_id,
      storage_directories.parsed_snapshot_id,
      storage_directories.snapshot_directory,
      snapshots.id as snapshot_id,
      snapshots.status as snapshot_status,
      snapshots.created_at as snapshot_created_at,
      snapshots.updated_at as snapshot_updated_at,
      snapshot_ttl.expires_at_utc as effective_expires_at,
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
  selected_snapshot_directories as (
    select classified.*
    from classified
    where classified.snapshot_id is not null
      and classified.reason in ('eligible_ttl_expired_snapshot', 'eligible_default_30d_snapshot')
    order by classified.newest_observed_at asc, classified.snapshot_id
    limit p_max_snapshots
  ),
  selected_orphan_directories as (
    select classified.*
    from classified
    where classified.snapshot_id is null
      and classified.parsed_snapshot_id is not null
      and classified.reason = 'eligible_orphan_storage_directory'
    order by classified.newest_observed_at asc, classified.snapshot_directory
    limit p_max_orphan_dirs
  ),
  selected_directories as (
    select
      'snapshot_directory'::text as candidate_type,
      true as delete_db_snapshot,
      selected_snapshot_directories.*
    from selected_snapshot_directories
    union all
    select
      'orphan_storage_directory'::text as candidate_type,
      false as delete_db_snapshot,
      selected_orphan_directories.*
    from selected_orphan_directories
  ),
  candidate_objects as (
    select
      selected_directories.candidate_type,
      coalesce(selected_directories.snapshot_id, selected_directories.parsed_snapshot_id) as snapshot_id,
      selected_directories.snapshot_directory,
      selected_directories.bucket_id,
      storage_objects.object_name,
      storage_objects.storage_bytes,
      selected_directories.reason,
      selected_directories.delete_db_snapshot,
      selected_directories.snapshot_status,
      selected_directories.snapshot_created_at,
      selected_directories.snapshot_updated_at,
      selected_directories.effective_expires_at,
      selected_directories.object_count,
      selected_directories.total_storage_bytes as snapshot_storage_bytes,
      selected_directories.active_ref_count,
      selected_directories.job_ref_count,
      selected_directories.result_ref_count,
      selected_directories.cache_ref_count,
      selected_directories.latest_ref_count,
      selected_directories.factorization_ref_count,
      selected_directories.artifact_ref_count,
      selected_directories.newest_observed_at
    from selected_directories
    join storage_objects
      on storage_objects.bucket_id = selected_directories.bucket_id
     and storage_objects.snapshot_directory = selected_directories.snapshot_directory
  ),
  budgeted as (
    select
      candidate_objects.*,
      sum(candidate_objects.storage_bytes) over (
        order by
          candidate_objects.candidate_type,
          candidate_objects.newest_observed_at asc,
          candidate_objects.object_name asc
      ) as cumulative_storage_bytes
    from candidate_objects
  )
  select
    budgeted.candidate_type,
    budgeted.snapshot_id,
    budgeted.snapshot_directory,
    budgeted.bucket_id,
    budgeted.object_name,
    budgeted.storage_bytes,
    budgeted.reason,
    budgeted.delete_db_snapshot,
    budgeted.snapshot_status,
    budgeted.snapshot_created_at,
    budgeted.snapshot_updated_at,
    budgeted.effective_expires_at,
    budgeted.object_count,
    budgeted.snapshot_storage_bytes,
    budgeted.active_ref_count as downstream_active_count,
    budgeted.job_ref_count as downstream_job_count,
    budgeted.result_ref_count as downstream_result_count,
    budgeted.cache_ref_count as downstream_cache_count,
    budgeted.latest_ref_count as downstream_latest_count,
    budgeted.factorization_ref_count as downstream_factorization_count,
    budgeted.artifact_ref_count as downstream_artifact_count
  from budgeted
  where budgeted.cumulative_storage_bytes <= p_max_bytes
  order by
    budgeted.candidate_type,
    budgeted.newest_observed_at asc,
    budgeted.object_name asc;
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
grant usage on schema util to service_role;
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
) is 'Operator snapshot GC candidate helper. Job references are counted from worker_jobs after public.lca_jobs retirement.';

drop table if exists
  public.lca_jobs,
  public.lca_package_jobs,
  public.dataset_review_submit_jobs
restrict;

comment on view public.worker_legacy_table_retirement_blockers
  is 'Service-role audit view for DROP TABLE RESTRICT blockers. It returns no target rows after public.lca_jobs, public.lca_package_jobs, and public.dataset_review_submit_jobs are physically retired.';

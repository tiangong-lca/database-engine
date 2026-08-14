CREATE OR REPLACE FUNCTION "util"."list_lca_snapshot_gc_candidates_without_closure_protection"("p_snapshot_retention_window" interval DEFAULT '30 days'::interval, "p_orphan_retention_window" interval DEFAULT '30 days'::interval, "p_as_of" timestamp with time zone DEFAULT "now"(), "p_max_snapshots" integer DEFAULT 100, "p_max_orphan_dirs" integer DEFAULT 200, "p_max_bytes" bigint DEFAULT '2147483648'::bigint) RETURNS TABLE("candidate_type" "text", "snapshot_id" "uuid", "snapshot_directory" "text", "bucket_id" "text", "object_name" "text", "storage_bytes" bigint, "reason" "text", "delete_db_snapshot" boolean, "snapshot_status" "text", "snapshot_created_at" timestamp with time zone, "snapshot_updated_at" timestamp with time zone, "effective_expires_at" timestamp with time zone, "object_count" bigint, "snapshot_storage_bytes" bigint, "downstream_active_count" bigint, "downstream_job_count" bigint, "downstream_result_count" bigint, "downstream_cache_count" bigint, "downstream_latest_count" bigint, "downstream_factorization_count" bigint, "downstream_artifact_count" bigint)
    LANGUAGE "plpgsql" STABLE
    SET "search_path" TO ''
    AS $_$
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
    from private.lca_active_snapshots
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
    from private.worker_jobs as jobs
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
    from private.lca_results
    group by lca_results.snapshot_id
  ),
  cache_refs as (
    select lca_result_cache.snapshot_id, count(*)::bigint as cache_ref_count
    from private.lca_result_cache
    group by lca_result_cache.snapshot_id
  ),
  latest_refs as (
    select lca_latest_all_unit_results.snapshot_id, count(*)::bigint as latest_ref_count
    from private.lca_latest_all_unit_results
    group by lca_latest_all_unit_results.snapshot_id
  ),
  factorization_refs as (
    select lca_factorization_registry.snapshot_id, count(*)::bigint as factorization_ref_count
    from private.lca_factorization_registry
    group by lca_factorization_registry.snapshot_id
  ),
  artifact_refs as (
    select lca_snapshot_artifacts.snapshot_id, count(*)::bigint as artifact_ref_count
    from private.lca_snapshot_artifacts
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
    left join private.lca_network_snapshots as snapshots
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
$_$;

ALTER FUNCTION "util"."list_lca_snapshot_gc_candidates_without_closure_protection"("p_snapshot_retention_window" interval, "p_orphan_retention_window" interval, "p_as_of" timestamp with time zone, "p_max_snapshots" integer, "p_max_orphan_dirs" integer, "p_max_bytes" bigint) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."list_lca_snapshot_gc_candidates_without_closure_protection"("p_snapshot_retention_window" interval, "p_orphan_retention_window" interval, "p_as_of" timestamp with time zone, "p_max_snapshots" integer, "p_max_orphan_dirs" integer, "p_max_bytes" bigint) FROM PUBLIC;

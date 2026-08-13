CREATE OR REPLACE FUNCTION "util"."preview_lca_snapshot_retention"("p_snapshot_retention_window" interval DEFAULT '30 days'::interval, "p_orphan_retention_window" interval DEFAULT '30 days'::interval, "p_as_of" timestamp with time zone DEFAULT "now"()) RETURNS TABLE("retention_area" "text", "retention_action" "text", "is_eligible" boolean, "reason" "text", "retention_window" interval, "cutoff_time" timestamp with time zone, "snapshot_count" bigint, "object_count" bigint, "total_storage_bytes" bigint, "downstream_active_count" bigint, "downstream_job_count" bigint, "downstream_result_count" bigint, "downstream_cache_count" bigint, "downstream_latest_count" bigint, "downstream_factorization_count" bigint, "downstream_artifact_count" bigint, "oldest_observed_at" timestamp with time zone, "newest_observed_at" timestamp with time zone)
    LANGUAGE "plpgsql" STABLE
    SET "search_path" TO ''
    AS $_$
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
$_$;

ALTER FUNCTION "util"."preview_lca_snapshot_retention"("p_snapshot_retention_window" interval, "p_orphan_retention_window" interval, "p_as_of" timestamp with time zone) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."preview_lca_snapshot_retention"("p_snapshot_retention_window" interval, "p_orphan_retention_window" interval, "p_as_of" timestamp with time zone) FROM PUBLIC;

GRANT ALL ON FUNCTION "util"."preview_lca_snapshot_retention"("p_snapshot_retention_window" interval, "p_orphan_retention_window" interval, "p_as_of" timestamp with time zone) TO "service_role";

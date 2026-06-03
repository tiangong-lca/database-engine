create or replace function util.preview_lca_package_retention(
  p_job_retention_window interval default interval '30 days',
  p_request_cache_retention_window interval default interval '30 days',
  p_as_of timestamp with time zone default pg_catalog.now()
) returns table (
  retention_area text,
  retention_action text,
  is_eligible boolean,
  reason text,
  retention_window interval,
  cutoff_time timestamp with time zone,
  row_count bigint,
  total_artifact_bytes bigint,
  total_hit_count bigint,
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
      message = 'package retention dry-run as_of timestamp must not be null';
  end if;

  if p_job_retention_window < interval '1 day' then
    raise exception using
      errcode = '22023',
      message = 'package job retention window must be at least 1 day';
  end if;

  if p_request_cache_retention_window < interval '1 day' then
    raise exception using
      errcode = '22023',
      message = 'package request-cache retention window must be at least 1 day';
  end if;

  return query
  with package_worker_jobs as (
    select
      jobs.id,
      jobs.status,
      coalesce(jobs.finished_at, jobs.updated_at, jobs.created_at) as lifecycle_at
    from public.worker_jobs as jobs
    where jobs.job_kind in ('tidas.export_package', 'tidas.import_package')
  ),
  job_rollup as (
    select
      jobs.id,
      jobs.status,
      jobs.lifecycle_at,
      coalesce(
        bool_or(artifacts.id is not null and artifacts.status <> 'deleted'),
        false
      ) as has_object_not_deleted,
      exists (
        select 1
        from public.lca_package_artifacts as recent_artifact
        join public.lca_package_request_cache as recent_cache
          on recent_cache.export_artifact_id = recent_artifact.id
          or recent_cache.report_artifact_id = recent_artifact.id
        where recent_artifact.worker_job_id = jobs.id
          and recent_cache.last_accessed_at >= p_as_of - p_request_cache_retention_window
      ) as has_recent_artifact_cache,
      coalesce(
        sum(coalesce(artifacts.artifact_byte_size, 0))
          filter (where artifacts.status <> 'deleted'),
        0
      )::bigint as artifact_bytes
    from package_worker_jobs as jobs
    left join public.lca_package_artifacts as artifacts
      on artifacts.worker_job_id = jobs.id
    group by jobs.id, jobs.status, jobs.lifecycle_at
  ),
  job_classified as (
    select
      'worker_jobs'::text as retention_area,
      'delete_package_worker_metadata_after_artifact_gc'::text as retention_action,
      (classified.reason = 'eligible_terminal_job_older_than_window') as is_eligible,
      classified.reason,
      p_job_retention_window as retention_window,
      p_as_of - p_job_retention_window as cutoff_time,
      1::bigint as row_count,
      classified.artifact_bytes as total_artifact_bytes,
      0::bigint as total_hit_count,
      classified.lifecycle_at as observed_at
    from (
      select
        job_rollup.*,
        case
          when job_rollup.status in ('queued', 'running', 'waiting') then 'protected_active_job'
          when job_rollup.lifecycle_at >= p_as_of - p_job_retention_window then 'protected_inside_job_retention_window'
          when job_rollup.has_object_not_deleted then 'protected_object_not_deleted'
          when job_rollup.has_recent_artifact_cache then 'protected_recent_request_cache_reference'
          else 'eligible_terminal_job_older_than_window'
        end as reason
      from job_rollup
    ) as classified
  ),
  artifact_classified as (
    select
      'lca_package_artifacts'::text as retention_area,
      'delete_object_then_mark_deleted'::text as retention_action,
      (classified.reason = 'eligible_expired_unpinned_artifact') as is_eligible,
      classified.reason,
      null::interval as retention_window,
      p_as_of as cutoff_time,
      1::bigint as row_count,
      coalesce(classified.artifact_byte_size, 0)::bigint as total_artifact_bytes,
      0::bigint as total_hit_count,
      coalesce(classified.expires_at, classified.updated_at, classified.created_at) as observed_at
    from (
      select
        artifacts.*,
        jobs.status as parent_job_status,
        coalesce(recent_cache.recent_cache_rows, 0) as recent_cache_rows,
        case
          when artifacts.is_pinned then 'protected_pinned_artifact'
          when artifacts.status = 'deleted' then 'protected_already_deleted'
          when artifacts.status = 'pending' then 'protected_artifact_not_ready'
          when jobs.id is null then 'protected_missing_worker_job'
          when jobs.status in ('queued', 'running', 'waiting') then 'protected_active_parent_job'
          when artifacts.expires_at is null then 'protected_missing_expires_at'
          when artifacts.expires_at > p_as_of then 'protected_expires_at_in_future'
          when coalesce(recent_cache.recent_cache_rows, 0) > 0 then 'protected_recent_request_cache_reference'
          else 'eligible_expired_unpinned_artifact'
        end as reason
      from public.lca_package_artifacts as artifacts
      left join package_worker_jobs as jobs
        on jobs.id = artifacts.worker_job_id
      left join lateral (
        select count(*)::bigint as recent_cache_rows
        from public.lca_package_request_cache as request_cache
        where (
            request_cache.export_artifact_id = artifacts.id
            or request_cache.report_artifact_id = artifacts.id
          )
          and request_cache.last_accessed_at >= p_as_of - p_request_cache_retention_window
      ) as recent_cache on true
    ) as classified
  ),
  request_cache_classified as (
    select
      'lca_package_request_cache'::text as retention_area,
      'delete_stale_request_cache_row'::text as retention_action,
      (classified.reason = 'eligible_stale_request_cache') as is_eligible,
      classified.reason,
      p_request_cache_retention_window as retention_window,
      p_as_of - p_request_cache_retention_window as cutoff_time,
      1::bigint as row_count,
      0::bigint as total_artifact_bytes,
      classified.hit_count::bigint as total_hit_count,
      classified.last_accessed_at as observed_at
    from (
      select
        request_cache.*,
        jobs.status as parent_job_status,
        case
          when request_cache.status in ('pending', 'running') then 'protected_active_request_cache'
          when request_cache.last_accessed_at >= p_as_of - p_request_cache_retention_window then 'protected_recent_request_cache_access'
          when jobs.status in ('queued', 'running', 'waiting') then 'protected_active_parent_job'
          else 'eligible_stale_request_cache'
        end as reason
      from public.lca_package_request_cache as request_cache
      left join package_worker_jobs as jobs
        on jobs.id = request_cache.worker_job_id
    ) as classified
  ),
  export_item_classified as (
    select
      'lca_package_export_items'::text as retention_area,
      'delete_export_items_with_parent_job'::text as retention_action,
      (classified.reason = 'eligible_parent_job_cascade') as is_eligible,
      classified.reason,
      p_job_retention_window as retention_window,
      p_as_of - p_job_retention_window as cutoff_time,
      1::bigint as row_count,
      0::bigint as total_artifact_bytes,
      0::bigint as total_hit_count,
      classified.created_at as observed_at
    from (
      select
        export_items.*,
        case
          when export_items.worker_job_id is null or job_rollup.id is null then 'protected_missing_worker_job'
          when job_rollup.status in ('queued', 'running', 'waiting') then 'protected_active_parent_job'
          when job_rollup.lifecycle_at >= p_as_of - p_job_retention_window then 'protected_parent_inside_job_retention_window'
          when job_rollup.has_object_not_deleted then 'protected_object_not_deleted'
          when job_rollup.has_recent_artifact_cache then 'protected_recent_request_cache_reference'
          else 'eligible_parent_job_cascade'
        end as reason
      from public.lca_package_export_items as export_items
      left join job_rollup
        on job_rollup.id = export_items.worker_job_id
    ) as classified
  ),
  classified as (
    select * from job_classified
    union all
    select * from artifact_classified
    union all
    select * from request_cache_classified
    union all
    select * from export_item_classified
  )
  select
    classified.retention_area,
    classified.retention_action,
    classified.is_eligible,
    classified.reason,
    classified.retention_window,
    classified.cutoff_time,
    sum(classified.row_count)::bigint as row_count,
    coalesce(sum(classified.total_artifact_bytes), 0)::bigint as total_artifact_bytes,
    coalesce(sum(classified.total_hit_count), 0)::bigint as total_hit_count,
    min(classified.observed_at) as oldest_observed_at,
    max(classified.observed_at) as newest_observed_at
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

alter function util.preview_lca_package_retention(interval, interval, timestamp with time zone) owner to postgres;
revoke all on function util.preview_lca_package_retention(interval, interval, timestamp with time zone) from public;
grant usage on schema util to service_role;
grant execute on function util.preview_lca_package_retention(interval, interval, timestamp with time zone) to service_role;

comment on function util.preview_lca_package_retention(interval, interval, timestamp with time zone) is
  'Operator dry-run helper for package metadata retention; reports aggregate eligible/protected counts for package worker jobs, artifacts, request cache, and export items without deleting or updating business rows.';

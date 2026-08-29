-- P0 safety rails for bounded, Worker-owned object-first package retention.

create or replace function util.apply_lca_package_retention(
  p_job_retention_window interval default interval '30 days',
  p_request_cache_retention_window interval default interval '30 days',
  p_as_of timestamp with time zone default pg_catalog.now(),
  p_max_rows integer default 1000,
  p_dry_run boolean default true
) returns table (
  retention_area text,
  retention_action text,
  dry_run boolean,
  affected_count bigint
)
language plpgsql
volatile
set search_path to ''
as $$
begin
  if p_as_of is null then
    raise exception using
      errcode = '22023',
      message = 'package retention apply as_of timestamp must not be null';
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

  if coalesce(p_max_rows, 0) <= 0 then
    raise exception using
      errcode = '22023',
      message = 'package retention max rows must be greater than zero';
  end if;

  if not p_dry_run then
    raise exception using
      errcode = '0A000',
      message = 'database-only package retention apply is disabled',
      detail = 'Use the Worker object-first package GC so object deletion succeeds before artifact tombstoning and dependent metadata cleanup.';
  end if;

  return query
  with artifact_candidates as (
    select artifacts.id
    from private.lca_package_artifacts as artifacts
    where artifacts.status = 'ready'
      and not artifacts.is_pinned
      and artifacts.expires_at is not null
      and artifacts.expires_at <= p_as_of
      and not exists (
        select 1
        from private.worker_jobs as active_job
        where active_job.status in ('queued', 'running', 'waiting')
          and (
            (
              artifacts.worker_job_id is not null
              and active_job.id = artifacts.worker_job_id
            )
            or active_job.payload_json ->> 'job_id' = artifacts.job_id::text
          )
      )
      and not exists (
        select 1
        from private.lca_package_request_cache as request_cache
        where (
            request_cache.export_artifact_id = artifacts.id
            or request_cache.report_artifact_id = artifacts.id
          )
          and (
            request_cache.status in ('pending', 'running')
            or request_cache.last_accessed_at >= p_as_of - p_request_cache_retention_window
          )
      )
    order by artifacts.expires_at, artifacts.created_at, artifacts.id
    limit p_max_rows
  ),
  request_cache_candidates as (
    select request_cache.id
    from private.lca_package_request_cache as request_cache
    where request_cache.status not in ('pending', 'running')
      and request_cache.last_accessed_at < p_as_of - p_request_cache_retention_window
      and not exists (
        select 1
        from private.worker_jobs as active_job
        where active_job.status in ('queued', 'running', 'waiting')
          and (
            (
              request_cache.worker_job_id is not null
              and active_job.id = request_cache.worker_job_id
            )
            or active_job.payload_json ->> 'job_id' = request_cache.job_id::text
          )
      )
      and not exists (
        select 1
        from private.lca_package_artifacts as artifacts
        where artifacts.status <> 'deleted'
          and artifacts.id in (
            request_cache.export_artifact_id,
            request_cache.report_artifact_id
          )
      )
    order by request_cache.last_accessed_at, request_cache.created_at, request_cache.id
    limit p_max_rows
  ),
  export_item_candidates as (
    select export_items.id
    from private.lca_package_export_items as export_items
    left join private.worker_jobs as canonical_job
      on canonical_job.id = export_items.worker_job_id
    where coalesce(
        canonical_job.finished_at,
        canonical_job.updated_at,
        canonical_job.created_at,
        export_items.created_at
      ) < p_as_of - p_job_retention_window
      and not exists (
        select 1
        from private.worker_jobs as active_job
        where active_job.status in ('queued', 'running', 'waiting')
          and (
            (
              export_items.worker_job_id is not null
              and active_job.id = export_items.worker_job_id
            )
            or active_job.payload_json ->> 'job_id' = export_items.job_id::text
          )
      )
      and not exists (
        select 1
        from private.lca_package_artifacts as artifacts
        where artifacts.status <> 'deleted'
          and (
            (
              export_items.worker_job_id is not null
              and artifacts.worker_job_id = export_items.worker_job_id
            )
            or artifacts.job_id = export_items.job_id
          )
      )
      and not exists (
        select 1
        from private.lca_package_request_cache as request_cache
        where (
            request_cache.status in ('pending', 'running')
            or request_cache.last_accessed_at >= p_as_of - p_request_cache_retention_window
          )
          and (
            (
              export_items.worker_job_id is not null
              and request_cache.worker_job_id = export_items.worker_job_id
            )
            or request_cache.job_id = export_items.job_id
          )
      )
    order by export_items.created_at, export_items.id
    limit p_max_rows
  )
  select
    'lca_package_artifacts'::text,
    'worker_object_delete_required'::text,
    true,
    count(*)::bigint
  from artifact_candidates
  union all
  select
    'lca_package_request_cache'::text,
    'worker_delete_stale_request_cache_rows'::text,
    true,
    count(*)::bigint
  from request_cache_candidates
  union all
  select
    'lca_package_export_items'::text,
    'worker_delete_export_items_after_artifact_gc'::text,
    true,
    count(*)::bigint
  from export_item_candidates;
end;
$$;

comment on function util.apply_lca_package_retention(
  interval, interval, timestamp with time zone, integer, boolean
)
  is 'Dry-run-only package retention preview. Mutating apply is fail-closed because only the Worker can prove object deletion before artifact tombstoning and dependent metadata cleanup.';

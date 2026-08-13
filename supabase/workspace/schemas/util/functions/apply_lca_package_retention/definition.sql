CREATE OR REPLACE FUNCTION "util"."apply_lca_package_retention"("p_job_retention_window" interval DEFAULT '30 days'::interval, "p_request_cache_retention_window" interval DEFAULT '30 days'::interval, "p_as_of" timestamp with time zone DEFAULT "now"(), "p_max_rows" integer DEFAULT 1000, "p_dry_run" boolean DEFAULT true) RETURNS TABLE("retention_area" "text", "retention_action" "text", "dry_run" boolean, "affected_count" bigint)
    LANGUAGE "plpgsql"
    SET "search_path" TO ''
    AS $$
declare
  v_count bigint;
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

  if p_dry_run then
    return query
    with artifact_candidates as (
      select artifacts.id
      from private.lca_package_artifacts as artifacts
      join private.worker_jobs as jobs
        on jobs.id = artifacts.worker_job_id
      where jobs.job_kind in ('tidas.export_package', 'tidas.import_package')
        and jobs.status not in ('queued', 'running', 'waiting')
        and artifacts.status not in ('deleted', 'pending')
        and not artifacts.is_pinned
        and artifacts.expires_at is not null
        and artifacts.expires_at <= p_as_of
        and not exists (
          select 1
          from private.lca_package_request_cache as request_cache
          where (
              request_cache.export_artifact_id = artifacts.id
              or request_cache.report_artifact_id = artifacts.id
            )
            and request_cache.last_accessed_at >= p_as_of - p_request_cache_retention_window
        )
      order by artifacts.expires_at, artifacts.created_at, artifacts.id
      limit p_max_rows
    ),
    request_cache_candidates as (
      select request_cache.id
      from private.lca_package_request_cache as request_cache
      left join private.worker_jobs as jobs
        on jobs.id = request_cache.worker_job_id
      where request_cache.status not in ('pending', 'running')
        and request_cache.last_accessed_at < p_as_of - p_request_cache_retention_window
        and coalesce(jobs.status not in ('queued', 'running', 'waiting'), true)
      order by request_cache.last_accessed_at, request_cache.created_at, request_cache.id
      limit p_max_rows
    ),
    export_item_candidates as (
      select export_items.id
      from private.lca_package_export_items as export_items
      join private.worker_jobs as jobs
        on jobs.id = export_items.worker_job_id
      where jobs.job_kind in ('tidas.export_package', 'tidas.import_package')
        and jobs.status not in ('queued', 'running', 'waiting')
        and coalesce(jobs.finished_at, jobs.updated_at, jobs.created_at) < p_as_of - p_job_retention_window
        and not exists (
          select 1
          from private.lca_package_artifacts as artifacts
          where artifacts.worker_job_id = export_items.worker_job_id
            and artifacts.status <> 'deleted'
        )
        and not exists (
          select 1
          from private.lca_package_request_cache as request_cache
          where request_cache.worker_job_id = export_items.worker_job_id
            and request_cache.last_accessed_at >= p_as_of - p_request_cache_retention_window
        )
      order by export_items.created_at, export_items.id
      limit p_max_rows
    )
    select
      'lca_package_artifacts'::text,
      'mark_expired_unpinned_artifacts_deleted'::text,
      true,
      count(*)::bigint
    from artifact_candidates
    union all
    select
      'lca_package_request_cache'::text,
      'delete_stale_request_cache_rows'::text,
      true,
      count(*)::bigint
    from request_cache_candidates
    union all
    select
      'lca_package_export_items'::text,
      'delete_export_items_after_artifact_gc'::text,
      true,
      count(*)::bigint
    from export_item_candidates;

    return;
  end if;

  with artifact_candidates as (
    select artifacts.id
    from private.lca_package_artifacts as artifacts
    join private.worker_jobs as jobs
      on jobs.id = artifacts.worker_job_id
    where jobs.job_kind in ('tidas.export_package', 'tidas.import_package')
      and jobs.status not in ('queued', 'running', 'waiting')
      and artifacts.status not in ('deleted', 'pending')
      and not artifacts.is_pinned
      and artifacts.expires_at is not null
      and artifacts.expires_at <= p_as_of
      and not exists (
        select 1
        from private.lca_package_request_cache as request_cache
        where (
            request_cache.export_artifact_id = artifacts.id
            or request_cache.report_artifact_id = artifacts.id
          )
          and request_cache.last_accessed_at >= p_as_of - p_request_cache_retention_window
      )
    order by artifacts.expires_at, artifacts.created_at, artifacts.id
    limit p_max_rows
  )
  update private.lca_package_artifacts as artifacts
     set status = 'deleted',
         metadata = artifacts.metadata || jsonb_build_object(
           'retentionDeletedAt', p_as_of,
           'retentionAction', 'package_metadata_retention_gc'
         ),
         updated_at = p_as_of
  from artifact_candidates
  where artifacts.id = artifact_candidates.id;

  get diagnostics v_count = row_count;
  return query select
    'lca_package_artifacts'::text,
    'mark_expired_unpinned_artifacts_deleted'::text,
    false,
    v_count;

  with request_cache_candidates as (
    select request_cache.id
    from private.lca_package_request_cache as request_cache
    left join private.worker_jobs as jobs
      on jobs.id = request_cache.worker_job_id
    where request_cache.status not in ('pending', 'running')
      and request_cache.last_accessed_at < p_as_of - p_request_cache_retention_window
      and coalesce(jobs.status not in ('queued', 'running', 'waiting'), true)
    order by request_cache.last_accessed_at, request_cache.created_at, request_cache.id
    limit p_max_rows
  )
  delete from private.lca_package_request_cache as request_cache
  using request_cache_candidates
  where request_cache.id = request_cache_candidates.id;

  get diagnostics v_count = row_count;
  return query select
    'lca_package_request_cache'::text,
    'delete_stale_request_cache_rows'::text,
    false,
    v_count;

  with export_item_candidates as (
    select export_items.id
    from private.lca_package_export_items as export_items
    join private.worker_jobs as jobs
      on jobs.id = export_items.worker_job_id
    where jobs.job_kind in ('tidas.export_package', 'tidas.import_package')
      and jobs.status not in ('queued', 'running', 'waiting')
      and coalesce(jobs.finished_at, jobs.updated_at, jobs.created_at) < p_as_of - p_job_retention_window
      and not exists (
        select 1
        from private.lca_package_artifacts as artifacts
        where artifacts.worker_job_id = export_items.worker_job_id
          and artifacts.status <> 'deleted'
      )
      and not exists (
        select 1
        from private.lca_package_request_cache as request_cache
        where request_cache.worker_job_id = export_items.worker_job_id
          and request_cache.last_accessed_at >= p_as_of - p_request_cache_retention_window
      )
    order by export_items.created_at, export_items.id
    limit p_max_rows
  )
  delete from private.lca_package_export_items as export_items
  using export_item_candidates
  where export_items.id = export_item_candidates.id;

  get diagnostics v_count = row_count;
  return query select
    'lca_package_export_items'::text,
    'delete_export_items_after_artifact_gc'::text,
    false,
    v_count;
end;
$$;

ALTER FUNCTION "util"."apply_lca_package_retention"("p_job_retention_window" interval, "p_request_cache_retention_window" interval, "p_as_of" timestamp with time zone, "p_max_rows" integer, "p_dry_run" boolean) OWNER TO "postgres";

REVOKE ALL ON FUNCTION "util"."apply_lca_package_retention"("p_job_retention_window" interval, "p_request_cache_retention_window" interval, "p_as_of" timestamp with time zone, "p_max_rows" integer, "p_dry_run" boolean) FROM PUBLIC;

GRANT ALL ON FUNCTION "util"."apply_lca_package_retention"("p_job_retention_window" interval, "p_request_cache_retention_window" interval, "p_as_of" timestamp with time zone, "p_max_rows" integer, "p_dry_run" boolean) TO "service_role";

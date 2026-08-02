-- Issue #390 Expand: stable service-only API facade for the retained
-- result/cache/latest capability family. Physical objects and the canonical
-- projection routines remain in public until the separately authorized
-- consumer-zero Contract migration.

create function api.lca_read_job_projection_v1(
  p_requested_by pg_catalog.uuid,
  p_worker_job_id pg_catalog.uuid,
  p_legacy_job_id pg_catalog.uuid,
  p_include_internal pg_catalog.bool
) returns pg_catalog.jsonb
language sql
security invoker
set search_path = ''
as $function$
  select public.lca_read_job_projection(
    p_requested_by,
    p_worker_job_id,
    p_legacy_job_id,
    p_include_internal
  )
$function$;

create function api.lca_read_result_projection_v1(
  p_requested_by pg_catalog.uuid,
  p_result_id pg_catalog.uuid,
  p_required_artifact_format pg_catalog.text,
  p_include_internal pg_catalog.bool
) returns pg_catalog.jsonb
language sql
security invoker
set search_path = ''
as $function$
  select public.lca_read_result_projection(
    p_requested_by,
    p_result_id,
    p_required_artifact_format,
    p_include_internal
  )
$function$;

create function api.lca_read_latest_single_solve_result_v1(
  p_requested_by pg_catalog.uuid,
  p_snapshot_id pg_catalog.uuid,
  p_process_index pg_catalog.int4
) returns pg_catalog.jsonb
language sql
security invoker
set search_path = ''
as $function$
  select public.lca_read_latest_single_solve_result(
    p_requested_by,
    p_snapshot_id,
    p_process_index
  )
$function$;

create function api.lca_read_result_cache_v1(
  p_scope pg_catalog.text,
  p_snapshot_id pg_catalog.uuid,
  p_request_key pg_catalog.text
) returns pg_catalog.jsonb
language sql
security invoker
set search_path = ''
as $function$
  select pg_catalog.jsonb_build_object(
    'ok', true,
    'data', (
      select pg_catalog.jsonb_build_object(
        'cacheId', cache_row.id,
        'scope', cache_row.scope,
        'snapshotId', cache_row.snapshot_id,
        'requestKey', cache_row.request_key,
        'status', cache_row.status,
        'legacyJobId', cache_row.job_id,
        'workerJobId', cache_row.worker_job_id,
        'resultId', cache_row.result_id,
        'hitCount', cache_row.hit_count,
        'lastAccessedAt', cache_row.last_accessed_at,
        'createdAt', cache_row.created_at,
        'updatedAt', cache_row.updated_at
      )
      from public.lca_result_cache as cache_row
      where cache_row.scope = p_scope
        and cache_row.snapshot_id = p_snapshot_id
        and cache_row.request_key = p_request_key
      limit 1
    )
  )
$function$;

create function api.cmd_lca_touch_result_cache_v1(
  p_cache_id pg_catalog.uuid
) returns pg_catalog.jsonb
language sql
security invoker
set search_path = ''
as $function$
  with touched as (
    update public.lca_result_cache as cache_row
    set hit_count = cache_row.hit_count + 1,
        last_accessed_at = pg_catalog.now(),
        updated_at = pg_catalog.now()
    where cache_row.id = p_cache_id
    returning cache_row.id,
              cache_row.status,
              cache_row.job_id,
              cache_row.worker_job_id,
              cache_row.result_id,
              cache_row.hit_count,
              cache_row.last_accessed_at,
              cache_row.updated_at
  )
  select pg_catalog.jsonb_build_object(
    'ok', true,
    'data', (
      select pg_catalog.jsonb_build_object(
        'cacheId', touched.id,
        'status', touched.status,
        'legacyJobId', touched.job_id,
        'workerJobId', touched.worker_job_id,
        'resultId', touched.result_id,
        'hitCount', touched.hit_count,
        'lastAccessedAt', touched.last_accessed_at,
        'updatedAt', touched.updated_at
      )
      from touched
    )
  )
$function$;

create function api.cmd_lca_admit_result_cache_v1(
  p_scope pg_catalog.text,
  p_snapshot_id pg_catalog.uuid,
  p_request_key pg_catalog.text,
  p_request_payload pg_catalog.jsonb,
  p_legacy_job_id pg_catalog.uuid,
  p_worker_job_id pg_catalog.uuid,
  p_replace_ready pg_catalog.bool
) returns pg_catalog.jsonb
language sql
security invoker
set search_path = ''
as $function$
  with admitted as (
    insert into public.lca_result_cache (
      scope,
      snapshot_id,
      request_key,
      request_payload,
      status,
      job_id,
      worker_job_id,
      result_id,
      error_code,
      error_message,
      hit_count,
      last_accessed_at,
      created_at,
      updated_at
    ) values (
      p_scope,
      p_snapshot_id,
      p_request_key,
      p_request_payload,
      'pending',
      p_legacy_job_id,
      p_worker_job_id,
      null,
      null,
      null,
      1,
      pg_catalog.now(),
      pg_catalog.now(),
      pg_catalog.now()
    )
    on conflict (scope, snapshot_id, request_key) do update
    set request_payload = case
          when (
            public.lca_result_cache.status in ('failed', 'stale')
            or (
              public.lca_result_cache.status = 'ready'
              and (
                public.lca_result_cache.result_id is null
                or p_replace_ready
              )
            )
            or (
              public.lca_result_cache.status in ('pending', 'running')
              and public.lca_result_cache.job_id is null
              and public.lca_result_cache.worker_job_id is null
            )
          )
            then excluded.request_payload
          else public.lca_result_cache.request_payload
        end,
        status = case
          when (
            public.lca_result_cache.status in ('failed', 'stale')
            or (
              public.lca_result_cache.status = 'ready'
              and (
                public.lca_result_cache.result_id is null
                or p_replace_ready
              )
            )
            or (
              public.lca_result_cache.status in ('pending', 'running')
              and public.lca_result_cache.job_id is null
              and public.lca_result_cache.worker_job_id is null
            )
          )
            then 'pending'
          else public.lca_result_cache.status
        end,
        job_id = case
          when (
            public.lca_result_cache.status in ('failed', 'stale')
            or (
              public.lca_result_cache.status = 'ready'
              and (
                public.lca_result_cache.result_id is null
                or p_replace_ready
              )
            )
            or (
              public.lca_result_cache.status in ('pending', 'running')
              and public.lca_result_cache.job_id is null
              and public.lca_result_cache.worker_job_id is null
            )
          )
            then excluded.job_id
          else public.lca_result_cache.job_id
        end,
        worker_job_id = case
          when (
            public.lca_result_cache.status in ('failed', 'stale')
            or (
              public.lca_result_cache.status = 'ready'
              and (
                public.lca_result_cache.result_id is null
                or p_replace_ready
              )
            )
            or (
              public.lca_result_cache.status in ('pending', 'running')
              and public.lca_result_cache.job_id is null
              and public.lca_result_cache.worker_job_id is null
            )
          )
            then excluded.worker_job_id
          else public.lca_result_cache.worker_job_id
        end,
        result_id = case
          when (
            public.lca_result_cache.status in ('failed', 'stale')
            or (
              public.lca_result_cache.status = 'ready'
              and (
                public.lca_result_cache.result_id is null
                or p_replace_ready
              )
            )
            or (
              public.lca_result_cache.status in ('pending', 'running')
              and public.lca_result_cache.job_id is null
              and public.lca_result_cache.worker_job_id is null
            )
          ) then null
          else public.lca_result_cache.result_id
        end,
        error_code = case
          when (
            public.lca_result_cache.status in ('failed', 'stale')
            or (
              public.lca_result_cache.status = 'ready'
              and (
                public.lca_result_cache.result_id is null
                or p_replace_ready
              )
            )
            or (
              public.lca_result_cache.status in ('pending', 'running')
              and public.lca_result_cache.job_id is null
              and public.lca_result_cache.worker_job_id is null
            )
          ) then null
          else public.lca_result_cache.error_code
        end,
        error_message = case
          when (
            public.lca_result_cache.status in ('failed', 'stale')
            or (
              public.lca_result_cache.status = 'ready'
              and (
                public.lca_result_cache.result_id is null
                or p_replace_ready
              )
            )
            or (
              public.lca_result_cache.status in ('pending', 'running')
              and public.lca_result_cache.job_id is null
              and public.lca_result_cache.worker_job_id is null
            )
          ) then null
          else public.lca_result_cache.error_message
        end,
        hit_count = public.lca_result_cache.hit_count + 1,
        last_accessed_at = excluded.last_accessed_at,
        updated_at = excluded.updated_at
    returning id,
              scope,
              snapshot_id,
              request_key,
              status,
              job_id,
              worker_job_id,
              result_id,
              hit_count,
              last_accessed_at,
              created_at,
              updated_at
  )
  select pg_catalog.jsonb_build_object(
    'ok', true,
    'outcome', case
      when admitted.status = 'pending'
        and admitted.job_id is not distinct from p_legacy_job_id
        and admitted.worker_job_id is not distinct from p_worker_job_id
        and admitted.result_id is null
        then 'accepted'
      else 'reused'
    end,
    'data', pg_catalog.jsonb_build_object(
      'cacheId', admitted.id,
      'scope', admitted.scope,
      'snapshotId', admitted.snapshot_id,
      'requestKey', admitted.request_key,
      'status', admitted.status,
      'legacyJobId', admitted.job_id,
      'workerJobId', admitted.worker_job_id,
      'resultId', admitted.result_id,
      'hitCount', admitted.hit_count,
      'lastAccessedAt', admitted.last_accessed_at,
      'createdAt', admitted.created_at,
      'updatedAt', admitted.updated_at
    )
  )
  from admitted
$function$;

create function api.cmd_lca_reconcile_result_cache_v1(
  p_requested_by pg_catalog.uuid,
  p_cache_id pg_catalog.uuid
) returns pg_catalog.jsonb
language sql
security invoker
set search_path = ''
as $function$
  with cache_state as (
    select cache_row.id,
           cache_row.status,
           cache_row.job_id,
           cache_row.worker_job_id,
           cache_row.result_id
    from public.lca_result_cache as cache_row
    where cache_row.id = p_cache_id
    for update
  ), projection as (
    select cache_state.id,
           cache_state.status as prior_status,
           cache_state.result_id as prior_result_id,
           public.lca_read_job_projection(
             p_requested_by,
             cache_state.worker_job_id,
             null,
             false
           ) as value
    from cache_state
  ), decision as (
    select projection.id,
           projection.prior_status,
           projection.prior_result_id,
           projection.value,
           projection.value #>> '{data,job,status}' as worker_status,
           case
             when pg_catalog.jsonb_typeof(
               projection.value #> '{data,result,resultId}'
             ) = 'string'
             and projection.value #>> '{data,result,resultId}' ~
               '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
               then (projection.value #>> '{data,result,resultId}')::pg_catalog.uuid
             else null
           end as projected_result_id,
           (
             pg_catalog.jsonb_typeof(projection.value->'data') = 'object'
             and pg_catalog.jsonb_typeof(projection.value #> '{data,job}') = 'object'
             and pg_catalog.jsonb_typeof(projection.value #> '{data,job,status}') = 'string'
             and (
               pg_catalog.jsonb_typeof(projection.value #> '{data,result}') is null
               or (
                 pg_catalog.jsonb_typeof(projection.value #> '{data,result}') = 'object'
                 and
                 pg_catalog.jsonb_typeof(projection.value #> '{data,result,resultId}') = 'string'
                 and projection.value #>> '{data,result,resultId}' ~
                   '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
               )
             )
           ) as projection_shape_valid
    from projection
  ), reconciled as (
    update public.lca_result_cache as cache_row
    set status = case
          when decision.worker_status = 'completed'
            and decision.projected_result_id is not null
            then 'ready'
          when decision.worker_status in ('failed', 'stale') then 'failed'
          else cache_row.status
        end,
        result_id = case
          when decision.worker_status = 'completed'
            and decision.projected_result_id is not null
            then decision.projected_result_id
          else cache_row.result_id
        end,
        hit_count = cache_row.hit_count + 1,
        last_accessed_at = pg_catalog.now(),
        updated_at = pg_catalog.now()
    from decision
    where cache_row.id = decision.id
      and decision.value->'ok' = 'true'::pg_catalog.jsonb
      and decision.projection_shape_valid
      and decision.worker_status in (
        'queued', 'running', 'waiting', 'completed', 'blocked',
        'stale', 'failed', 'cancelled'
      )
    returning cache_row.id,
              cache_row.status,
              cache_row.job_id,
              cache_row.worker_job_id,
              cache_row.result_id,
              cache_row.hit_count,
              cache_row.last_accessed_at,
              cache_row.updated_at,
              decision.worker_status,
              decision.projected_result_id,
              decision.value
  )
  select coalesce(
    (
      select case
        when decision.value->'ok' = 'false'::pg_catalog.jsonb
          and pg_catalog.jsonb_typeof(decision.value->'code') = 'string'
          and pg_catalog.length(decision.value->>'code') > 0
          and pg_catalog.jsonb_typeof(decision.value->'message') = 'string'
          and pg_catalog.length(decision.value->>'message') > 0
          and pg_catalog.jsonb_typeof(decision.value->'status') = 'number'
          and decision.value #>> '{status}' ~ '^[1-5][0-9][0-9]$' then
          pg_catalog.jsonb_strip_nulls(pg_catalog.jsonb_build_object(
            'ok', false,
            'code', decision.value->>'code',
            'status', decision.value->'status',
            'message', decision.value->>'message',
            'details', decision.value->'details',
            'data', null
          ))
        when decision.value->'ok' = 'true'::pg_catalog.jsonb
          and decision.value ? 'data'
          and decision.value->'data' = 'null'::pg_catalog.jsonb then
          pg_catalog.jsonb_build_object(
            'ok', true,
            'code', 'job_not_found',
            'data', null
          )
        when decision.value->'ok' = 'true'::pg_catalog.jsonb
          and decision.projection_shape_valid
          and decision.worker_status in (
            'queued', 'running', 'waiting', 'completed', 'blocked',
            'stale', 'failed', 'cancelled'
          ) then
          pg_catalog.jsonb_build_object(
        'ok', true,
        'code', case
          when reconciled.worker_status = 'completed'
            and reconciled.projected_result_id is null
            then 'result_pending'
          else 'reconciled'
        end,
        'data', pg_catalog.jsonb_build_object(
            'cache', pg_catalog.jsonb_build_object(
              'cacheId', reconciled.id,
              'status', reconciled.status,
              'legacyJobId', reconciled.job_id,
              'workerJobId', reconciled.worker_job_id,
              'resultId', reconciled.result_id,
              'hitCount', reconciled.hit_count,
              'lastAccessedAt', reconciled.last_accessed_at,
              'updatedAt', reconciled.updated_at
            ),
            'workerStatus', reconciled.worker_status,
            'jobProjection', reconciled.value
          )
      )
        else pg_catalog.jsonb_build_object(
          'ok', false,
          'code', 'INVALID_LCA_JOB_PROJECTION',
          'status', 500,
          'data', null
        )
      end
      from decision
      left join reconciled on reconciled.id = decision.id
    ),
    pg_catalog.jsonb_build_object(
      'ok', true,
      'code', 'cache_not_found',
      'data', null
    )
  )
$function$;

create function api.lca_read_latest_all_unit_result_v1(
  p_snapshot_id pg_catalog.uuid
) returns pg_catalog.jsonb
language sql
security invoker
set search_path = ''
as $function$
  select pg_catalog.jsonb_build_object(
    'ok', true,
    'data', (
      select pg_catalog.jsonb_build_object(
        'snapshotId', latest.snapshot_id,
        'resultId', latest.result_id,
        'computedAt', latest.computed_at,
        'queryArtifactUrl', latest.query_artifact_url,
        'queryArtifactFormat', latest.query_artifact_format
      )
      from public.lca_latest_all_unit_results as latest
      where latest.snapshot_id = p_snapshot_id
        and latest.status = 'ready'
      order by latest.updated_at desc
      limit 1
    )
  )
$function$;

revoke all on function api.lca_read_job_projection_v1(
  pg_catalog.uuid,
  pg_catalog.uuid,
  pg_catalog.uuid,
  pg_catalog.bool
) from public, anon, authenticated, api_internal_executor;
revoke all on function api.lca_read_result_projection_v1(
  pg_catalog.uuid,
  pg_catalog.uuid,
  pg_catalog.text,
  pg_catalog.bool
) from public, anon, authenticated, api_internal_executor;
revoke all on function api.lca_read_latest_single_solve_result_v1(
  pg_catalog.uuid,
  pg_catalog.uuid,
  pg_catalog.int4
) from public, anon, authenticated, api_internal_executor;
revoke all on function api.lca_read_result_cache_v1(
  pg_catalog.text,
  pg_catalog.uuid,
  pg_catalog.text
) from public, anon, authenticated, api_internal_executor;
revoke all on function api.cmd_lca_touch_result_cache_v1(
  pg_catalog.uuid
) from public, anon, authenticated, api_internal_executor;
revoke all on function api.cmd_lca_admit_result_cache_v1(
  pg_catalog.text,
  pg_catalog.uuid,
  pg_catalog.text,
  pg_catalog.jsonb,
  pg_catalog.uuid,
  pg_catalog.uuid,
  pg_catalog.bool
) from public, anon, authenticated, api_internal_executor;
revoke all on function api.cmd_lca_reconcile_result_cache_v1(
  pg_catalog.uuid,
  pg_catalog.uuid
) from public, anon, authenticated, api_internal_executor;
revoke all on function api.lca_read_latest_all_unit_result_v1(
  pg_catalog.uuid
) from public, anon, authenticated, api_internal_executor;

grant execute on function api.lca_read_job_projection_v1(
  pg_catalog.uuid,
  pg_catalog.uuid,
  pg_catalog.uuid,
  pg_catalog.bool
) to service_role;
grant execute on function api.lca_read_result_projection_v1(
  pg_catalog.uuid,
  pg_catalog.uuid,
  pg_catalog.text,
  pg_catalog.bool
) to service_role;
grant execute on function api.lca_read_latest_single_solve_result_v1(
  pg_catalog.uuid,
  pg_catalog.uuid,
  pg_catalog.int4
) to service_role;
grant execute on function api.lca_read_result_cache_v1(
  pg_catalog.text,
  pg_catalog.uuid,
  pg_catalog.text
) to service_role;
grant execute on function api.cmd_lca_touch_result_cache_v1(
  pg_catalog.uuid
) to service_role;
grant execute on function api.cmd_lca_admit_result_cache_v1(
  pg_catalog.text,
  pg_catalog.uuid,
  pg_catalog.text,
  pg_catalog.jsonb,
  pg_catalog.uuid,
  pg_catalog.uuid,
  pg_catalog.bool
) to service_role;
grant execute on function api.cmd_lca_reconcile_result_cache_v1(
  pg_catalog.uuid,
  pg_catalog.uuid
) to service_role;
grant execute on function api.lca_read_latest_all_unit_result_v1(
  pg_catalog.uuid
) to service_role;

comment on function api.lca_read_job_projection_v1(
  pg_catalog.uuid,
  pg_catalog.uuid,
  pg_catalog.uuid,
  pg_catalog.bool
) is 'Issue #390 v1 service-only LCA job projection facade.';
comment on function api.lca_read_result_projection_v1(
  pg_catalog.uuid,
  pg_catalog.uuid,
  pg_catalog.text,
  pg_catalog.bool
) is 'Issue #390 v1 service-only LCA result projection facade.';
comment on function api.lca_read_latest_single_solve_result_v1(
  pg_catalog.uuid,
  pg_catalog.uuid,
  pg_catalog.int4
) is 'Issue #390 v1 service-only latest single-solve projection facade.';
comment on function api.lca_read_result_cache_v1(
  pg_catalog.text,
  pg_catalog.uuid,
  pg_catalog.text
) is 'Issue #390 v1 service-only result-cache lookup facade.';
comment on function api.cmd_lca_touch_result_cache_v1(
  pg_catalog.uuid
) is 'Issue #390 v1 service-only atomic result-cache touch command.';
comment on function api.cmd_lca_admit_result_cache_v1(
  pg_catalog.text,
  pg_catalog.uuid,
  pg_catalog.text,
  pg_catalog.jsonb,
  pg_catalog.uuid,
  pg_catalog.uuid,
  pg_catalog.bool
) is 'Issue #390 v1 service-only atomic admission. outcome=accepted means the returned pending binding matches the request (insert, failed/stale reset, broken-ready/no-job recovery, or same-binding replay); outcome=reused preserves a different canonical binding. p_replace_ready=false preserves valid ready, while true replaces valid ready for all-unit admission; pending/running with either job identity is always preserved. Edge must choose touch, admit, or reconcile as mutually exclusive branches; each command increments hit_count exactly once.';
comment on function api.cmd_lca_reconcile_result_cache_v1(
  pg_catalog.uuid,
  pg_catalog.uuid
) is 'Issue #390 v1 service-only cache reconciliation command. cache_not_found, job_not_found, and projection failure preserve the entire cache row without a touch; failures preserve code/status/message/details with ok=false. Completed jobs without a visible result return result_pending, preserve pending state, and touch once for retry convergence. Edge must not call touch in the same request branch.';
comment on function api.lca_read_latest_all_unit_result_v1(
  pg_catalog.uuid
) is 'Issue #390 v1 service-only latest all-unit result projection facade.';

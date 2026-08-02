-- Issue #395: a cancelled Worker job is terminal. Reconcile it to a failed
-- cache row atomically so a later request can enqueue and admit a retry.
-- This is a forward-only replacement of the existing service-only v1 facade;
-- it does not add a routine or query surface.

create or replace function api.cmd_lca_reconcile_result_cache_v1(
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
          when decision.worker_status in ('failed', 'stale', 'cancelled') then 'failed'
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

revoke all on function api.cmd_lca_reconcile_result_cache_v1(
  pg_catalog.uuid,
  pg_catalog.uuid
) from public, anon, authenticated, service_role, api_internal_executor;

grant execute on function api.cmd_lca_reconcile_result_cache_v1(
  pg_catalog.uuid,
  pg_catalog.uuid
) to service_role;

comment on function api.cmd_lca_reconcile_result_cache_v1(
  pg_catalog.uuid,
  pg_catalog.uuid
) is 'Issue #395 service-only cache reconciliation command. cancelled, failed, and stale Worker states atomically converge cache status to failed while preserving job/result identity and touching exactly once. cache_not_found, job_not_found, and projection failures preserve the complete cache row. Completed jobs without a visible result return result_pending. Edge must not call touch or admit in the same request branch.';

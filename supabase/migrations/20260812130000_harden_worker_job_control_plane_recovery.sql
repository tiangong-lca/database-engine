-- Keep queue polling non-blocking when another transaction owns an expired row,
-- and make ambiguous terminal-result retries safe for the same lease.

create or replace function private.worker_claim_jobs(
  p_worker_queue text,
  p_worker_id text default null,
  p_limit integer default 10,
  p_lease_seconds integer default null
) returns jsonb
language plpgsql
security definer
set search_path = private, api, public, util, extensions, pg_temp
as $$
declare
  v_worker_queue text := lower(trim(coalesce(p_worker_queue, '')));
  v_worker_id text := nullif(trim(p_worker_id), '');
  v_limit integer := greatest(1, least(coalesce(p_limit, 10), 50));
  v_lease_seconds integer := greatest(1, least(coalesce(p_lease_seconds, 300), 86400));
  v_jobs jsonb := '[]'::jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to claim worker jobs'
    );
  end if;

  if v_worker_queue not in ('solver', 'review_submit', 'review_submit_gate', 'package', 'maintenance') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_QUEUE',
      'status', 400,
      'message', 'workerQueue must be solver, review_submit, review_submit_gate, package, or maintenance'
    );
  end if;

  with expired_candidates as (
    select id
    from private.worker_jobs
    where worker_runtime = 'calculator'
      and worker_queue = v_worker_queue
      and status = 'running'
      and lease_expires_at < now()
      and attempt_count >= max_attempts
    order by lease_expires_at asc, created_at asc
    limit v_limit
    for update skip locked
  ),
  expired as (
    update private.worker_jobs as j
      set status = 'failed',
          error_code = coalesce(j.error_code, 'lease_expired_max_attempts'),
          error_message = coalesce(j.error_message, 'Worker job lease expired after the maximum attempt count'),
          error_details = coalesce(j.error_details, '{}'::jsonb) || jsonb_build_object(
            'leasedBy', j.leased_by,
            'leaseExpiresAt', j.lease_expires_at,
            'attemptCount', j.attempt_count,
            'maxAttempts', j.max_attempts
          ),
          leased_by = null,
          lease_token = null,
          lease_expires_at = null,
          heartbeat_at = coalesce(j.heartbeat_at, now()),
          updated_at = now(),
          finished_at = now()
    from expired_candidates
    where j.id = expired_candidates.id
    returning j.*
  ),
  expired_events as (
    insert into private.worker_job_events (
      job_id,
      event_type,
      status,
      worker_id,
      message,
      details
    )
    select
      expired.id,
      'failed',
      expired.status,
      expired.leased_by,
      'Worker job lease expired after the maximum attempt count',
      jsonb_build_object(
        'errorCode', expired.error_code,
        'attemptCount', expired.attempt_count,
        'maxAttempts', expired.max_attempts
      )
    from expired
    returning id
  ),
  candidate as (
    select id
    from private.worker_jobs
    where worker_runtime = 'calculator'
      and worker_queue = v_worker_queue
      and run_after <= now()
      and attempt_count < max_attempts
      and (
        status in ('queued', 'stale')
        or (status = 'running' and lease_expires_at < now())
      )
    order by priority desc, run_after asc, created_at asc
    limit v_limit
    for update skip locked
  ),
  updated as (
    update private.worker_jobs as j
      set status = 'running',
          attempt_count = j.attempt_count + 1,
          leased_by = v_worker_id,
          lease_token = gen_random_uuid(),
          lease_expires_at = now() + make_interval(secs => v_lease_seconds),
          heartbeat_at = now(),
          started_at = coalesce(j.started_at, now()),
          updated_at = now(),
          diagnostics = case
            when j.attempt_count > 0 then '{}'::jsonb
            else j.diagnostics
          end,
          error_code = null,
          error_message = null,
          error_details = null
    from candidate
    where j.id = candidate.id
    returning j.*
  ),
  claim_events as (
    insert into private.worker_job_events (
      job_id,
      event_type,
      status,
      phase,
      progress,
      worker_id,
      lease_token,
      details
    )
    select
      updated.id,
      'claimed',
      updated.status,
      updated.phase,
      updated.progress,
      updated.leased_by,
      updated.lease_token,
      jsonb_build_object(
        'attemptCount', updated.attempt_count,
        'leaseExpiresAt', updated.lease_expires_at
      )
    from updated
    returning id
  )
  select coalesce(jsonb_agg(private.worker_job_payload(updated, true)), '[]'::jsonb)
    into v_jobs
  from updated;

  return jsonb_build_object(
    'ok', true,
    'data', v_jobs
  );
end;
$$;

create or replace function private.worker_record_job_result(
  p_job_id uuid,
  p_lease_token uuid,
  p_status text,
  p_result_json jsonb default null,
  p_result_schema_version text default null,
  p_result_ref jsonb default null,
  p_diagnostics jsonb default null,
  p_error_code text default null,
  p_error_message text default null,
  p_error_details jsonb default null,
  p_blocker_codes text[] default null,
  p_resolution_scope text default null,
  p_retryable boolean default null
) returns jsonb
language plpgsql
security definer
set search_path = private, api, public, util, extensions, pg_temp
as $$
declare
  v_status text := lower(trim(coalesce(p_status, '')));
  v_resolution_scope text := lower(trim(coalesce(p_resolution_scope, '')));
  v_blocker_codes text[] := coalesce(p_blocker_codes, '{}'::text[]);
  v_job private.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to record worker job results'
    );
  end if;

  if v_status not in ('completed', 'blocked', 'failed', 'waiting') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_RESULT_STATUS',
      'status', 400,
      'message', 'status must be completed, blocked, failed, or waiting'
    );
  end if;

  if p_result_json is not null and jsonb_typeof(p_result_json) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_RESULT',
      'status', 400,
      'message', 'result must be a JSON object'
    );
  end if;

  if p_result_ref is not null and jsonb_typeof(p_result_ref) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_RESULT_REF',
      'status', 400,
      'message', 'resultRef must be a JSON object'
    );
  end if;

  if p_diagnostics is not null and jsonb_typeof(p_diagnostics) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_DIAGNOSTICS',
      'status', 400,
      'message', 'diagnostics must be a JSON object'
    );
  end if;

  if p_error_details is not null and jsonb_typeof(p_error_details) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_ERROR_DETAILS',
      'status', 400,
      'message', 'error details must be a JSON object'
    );
  end if;

  if v_status = 'blocked' and (cardinality(v_blocker_codes) = 0 or v_resolution_scope = '') then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_BLOCKER_DETAILS_REQUIRED',
      'status', 400,
      'message', 'blocked worker jobs require blockerCodes and resolutionScope'
    );
  end if;

  if v_status = 'blocked' and v_resolution_scope not in ('user', 'operator', 'system') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_RESOLUTION_SCOPE',
      'status', 400,
      'message', 'resolutionScope must be user, operator, or system'
    );
  end if;

  if v_status = 'failed' and nullif(trim(p_error_code), '') is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_ERROR_CODE_REQUIRED',
      'status', 400,
      'message', 'failed worker jobs require an errorCode'
    );
  end if;

  select *
    into v_job
  from private.worker_jobs
  where id = p_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Worker job not found'
    );
  end if;

  if v_job.status <> 'running' then
    if v_job.status = v_status
      and v_job.result_json is not distinct from p_result_json
      and v_job.result_schema_version is not distinct from coalesce(
        nullif(trim(p_result_schema_version), ''),
        v_job.result_schema_version
      )
      and v_job.result_ref is not distinct from p_result_ref
      and v_job.diagnostics is not distinct from coalesce(p_diagnostics, '{}'::jsonb)
      and v_job.error_code is not distinct from nullif(trim(p_error_code), '')
      and v_job.error_message is not distinct from nullif(trim(p_error_message), '')
      and v_job.error_details is not distinct from p_error_details
      and v_job.blocker_codes is not distinct from (case
        when v_status = 'blocked' then v_blocker_codes
        else '{}'::text[]
      end)
      and v_job.resolution_scope is not distinct from (case
        when v_status = 'blocked' then v_resolution_scope
        else null
      end)
      and v_job.retryable is not distinct from p_retryable
      and exists (
        select 1
        from private.worker_job_events as event
        where event.job_id = v_job.id
          and event.event_type = v_status
          and event.lease_token is not distinct from p_lease_token
      )
    then
      return jsonb_build_object(
        'ok', true,
        'data', private.worker_job_payload(v_job, true),
        'idempotentReplay', true
      );
    end if;

    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_RUNNING',
      'status', 409,
      'message', 'Worker job is not running'
    );
  end if;

  if v_job.lease_token is distinct from p_lease_token then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_LEASE_TOKEN_MISMATCH',
      'status', 409,
      'message', 'Worker job lease token does not match'
    );
  end if;

  if v_job.lease_expires_at < now() then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_LEASE_EXPIRED',
      'status', 409,
      'message', 'Worker job lease has expired'
    );
  end if;

  update private.worker_jobs
    set status = v_status,
        progress = case
          when v_status = 'completed' then 1
          else progress
        end,
        result_schema_version = coalesce(nullif(trim(p_result_schema_version), ''), result_schema_version),
        result_json = p_result_json,
        result_ref = p_result_ref,
        diagnostics = coalesce(p_diagnostics, '{}'::jsonb),
        error_code = nullif(trim(p_error_code), ''),
        error_message = nullif(trim(p_error_message), ''),
        error_details = p_error_details,
        blocker_codes = case
          when v_status = 'blocked' then v_blocker_codes
          else '{}'::text[]
        end,
        resolution_scope = case
          when v_status = 'blocked' then v_resolution_scope
          else null
        end,
        retryable = p_retryable,
        leased_by = null,
        lease_token = null,
        lease_expires_at = null,
        heartbeat_at = coalesce(heartbeat_at, now()),
        updated_at = now(),
        finished_at = case
          when v_status in ('completed', 'blocked', 'failed') then now()
          else null
        end
  where id = v_job.id
  returning *
    into v_job;

  insert into private.worker_job_events (
    job_id,
    event_type,
    status,
    phase,
    progress,
    worker_id,
    lease_token,
    message,
    details
  ) values (
    v_job.id,
    v_status,
    v_job.status,
    v_job.phase,
    v_job.progress,
    null,
    p_lease_token,
    coalesce(p_error_message, null),
    jsonb_strip_nulls(
      jsonb_build_object(
        'errorCode', v_job.error_code,
        'blockerCodes', to_jsonb(v_job.blocker_codes),
        'resolutionScope', v_job.resolution_scope,
        'retryable', v_job.retryable
      )
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', private.worker_job_payload(v_job, true)
  );
end;
$$;

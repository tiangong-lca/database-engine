-- Register the reusable AI worker queue and its first versioned handler.
-- Edge remains the authenticated request boundary; only service-role facades
-- can enqueue or read a user's AI job.

alter table private.worker_job_kinds
  drop constraint if exists worker_job_kinds_queue_check;

alter table private.worker_job_kinds
  add constraint worker_job_kinds_queue_check
  check (
    worker_queue in (
      'solver',
      'review_submit',
      'review_submit_gate',
      'review_quality',
      'package',
      'maintenance',
      'ai'
    )
  );

alter table private.worker_jobs
  drop constraint if exists worker_jobs_queue_check;

alter table private.worker_jobs
  add constraint worker_jobs_queue_check
  check (
    worker_queue in (
      'solver',
      'review_submit',
      'review_submit_gate',
      'review_quality',
      'package',
      'maintenance',
      'ai'
    )
  );

insert into private.worker_job_kinds (
  job_kind,
  worker_runtime,
  worker_queue,
  default_visibility,
  default_priority,
  default_max_attempts,
  default_lease_seconds,
  payload_schema_version,
  result_schema_version,
  user_visible,
  description,
  task_center_category,
  task_center_surface,
  presenter_key
) values (
  'ai.tidas_suggestion',
  'calculator',
  'ai',
  'user',
  0,
  3,
  900,
  'ai.tidas_suggestion.request.v1',
  'ai.tidas_suggestion.result.v1',
  false,
  'Advisory TIDAS Process or Flow improvement handled by the reusable AI worker.',
  null,
  null,
  null
) on conflict (job_kind) do update
set worker_runtime = excluded.worker_runtime,
    worker_queue = excluded.worker_queue,
    default_visibility = excluded.default_visibility,
    default_priority = excluded.default_priority,
    default_max_attempts = excluded.default_max_attempts,
    default_lease_seconds = excluded.default_lease_seconds,
    payload_schema_version = excluded.payload_schema_version,
    result_schema_version = excluded.result_schema_version,
    user_visible = excluded.user_visible,
    description = excluded.description,
    task_center_category = excluded.task_center_category,
    task_center_surface = excluded.task_center_surface,
    presenter_key = excluded.presenter_key,
    updated_at = now();

alter table private.worker_jobs
  drop constraint if exists worker_jobs_ai_tidas_suggestion_semantics_check;

alter table private.worker_jobs
  add constraint worker_jobs_ai_tidas_suggestion_semantics_check
  check (
    job_kind <> 'ai.tidas_suggestion'
    or (
      worker_queue = 'ai'
      and visibility = 'user'
      and payload_schema_version = 'ai.tidas_suggestion.request.v1'
      and coalesce(jsonb_typeof(payload_json), '') = 'object'
      and coalesce(payload_json->>'dataType', '') in ('process', 'flow')
      and coalesce(jsonb_typeof(payload_json->'data'), '') = 'object'
      and (
        (payload_json->>'dataType' = 'process'
          and coalesce(
            jsonb_typeof(payload_json->'data'->'processDataSet'),
            ''
          ) = 'object')
        or
        (payload_json->>'dataType' = 'flow'
          and coalesce(
            jsonb_typeof(payload_json->'data'->'flowDataSet'),
            ''
          ) = 'object')
      )
      and status <> 'blocked'
      and cardinality(blocker_codes) = 0
      and resolution_scope is null
      and (
        status <> 'completed'
        or (
          result_schema_version = 'ai.tidas_suggestion.result.v1'
          and coalesce(jsonb_typeof(result_json), '') = 'object'
          and coalesce(result_json->>'status', '') in ('complete', 'partial')
        )
      )
      and (
        status <> 'failed'
        or result_json is null
        or (
          result_schema_version = 'ai.tidas_suggestion.result.v1'
          and coalesce(jsonb_typeof(result_json), '') = 'object'
          and coalesce(result_json->>'status', '') = 'failed'
        )
      )
    )
  );

create index if not exists worker_jobs_ai_tidas_suggestion_requester_idx
  on private.worker_jobs (requested_by, updated_at desc, id desc)
  where job_kind = 'ai.tidas_suggestion';

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

  if v_worker_queue not in (
    'solver', 'review_submit', 'review_submit_gate', 'review_quality',
    'package', 'maintenance', 'ai'
  ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_QUEUE',
      'status', 400,
      'message', 'Unsupported worker queue'
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
          error_message = coalesce(
            j.error_message,
            'Worker job lease expired after the maximum attempt count'
          ),
          error_details = coalesce(j.error_details, '{}'::jsonb)
            || jsonb_build_object(
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
      job_id, event_type, status, worker_id, message, details
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
      job_id, event_type, status, phase, progress, worker_id, lease_token, details
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
  select coalesce(
    jsonb_agg(private.worker_job_payload(updated, true)),
    '[]'::jsonb
  ) into v_jobs
  from updated;

  return jsonb_build_object('ok', true, 'data', v_jobs);
end;
$$;

alter function private.worker_claim_jobs(text, text, integer, integer)
  owner to postgres;

revoke all on function private.worker_claim_jobs(text, text, integer, integer)
  from public, anon, authenticated;

grant execute on function private.worker_claim_jobs(text, text, integer, integer)
  to service_role, api_internal_executor;

create or replace function api.svc_ai_tidas_suggestion_enqueue(
  p_requested_by uuid,
  p_data_type text,
  p_data jsonb
) returns jsonb
language plpgsql
volatile
security definer
set search_path = ''
as $$
declare
  v_data_type text := lower(trim(coalesce(p_data_type, '')));
  v_payload jsonb;
  v_request_hash text;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to enqueue AI jobs'
    );
  end if;

  if p_requested_by is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AI_REQUESTED_BY_REQUIRED',
      'status', 400,
      'message', 'requestedBy is required'
    );
  end if;

  if v_data_type not in ('process', 'flow') then
    return jsonb_build_object(
      'ok', false,
      'code', 'AI_DATA_TYPE_INVALID',
      'status', 400,
      'message', 'dataType must be process or flow'
    );
  end if;

  if jsonb_typeof(p_data) is distinct from 'object'
    or (
      v_data_type = 'process'
      and jsonb_typeof(p_data->'processDataSet') is distinct from 'object'
    )
    or (
      v_data_type = 'flow'
      and jsonb_typeof(p_data->'flowDataSet') is distinct from 'object'
    ) then
    return jsonb_build_object(
      'ok', false,
      'code', 'AI_DATA_INVALID',
      'status', 400,
      'message', 'data must contain the matching TIDAS dataset root object'
    );
  end if;

  v_payload := jsonb_build_object(
    'dataType', v_data_type,
    'data', p_data
  );

  if pg_catalog.octet_length(v_payload::text) > 16777216 then
    return jsonb_build_object(
      'ok', false,
      'code', 'AI_DATA_TOO_LARGE',
      'status', 413,
      'message', 'AI request exceeds the 16 MiB absolute contract limit'
    );
  end if;

  v_request_hash := pg_catalog.encode(
    extensions.digest(
      pg_catalog.convert_to(v_payload::text, 'UTF8'),
      'sha256'
    ),
    'hex'
  );

  return private.worker_enqueue_job(
    p_job_kind => 'ai.tidas_suggestion',
    p_payload_json => v_payload,
    p_payload_schema_version => 'ai.tidas_suggestion.request.v1',
    p_requested_by => p_requested_by,
    p_requester_type => 'user',
    p_idempotency_key => 'ai.tidas_suggestion:' || v_request_hash,
    p_request_hash => v_request_hash,
    p_visibility => 'user',
    p_max_attempts => 3,
    p_timeout_at => now() + interval '30 minutes'
  );
end;
$$;

create or replace function api.svc_ai_tidas_suggestion_read(
  p_requested_by uuid,
  p_job_id uuid
) returns jsonb
language plpgsql
stable
security definer
set search_path = ''
as $$
declare
  v_job private.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to read AI jobs'
    );
  end if;

  if p_requested_by is null or p_job_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AI_JOB_NOT_FOUND',
      'status', 404,
      'message', 'AI job was not found'
    );
  end if;

  select * into v_job
  from private.worker_jobs
  where id = p_job_id
    and requested_by = p_requested_by
    and job_kind = 'ai.tidas_suggestion';

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AI_JOB_NOT_FOUND',
      'status', 404,
      'message', 'AI job was not found'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', private.worker_job_payload(v_job, false)
  );
end;
$$;

alter function api.svc_ai_tidas_suggestion_enqueue(uuid, text, jsonb)
  owner to postgres;
alter function api.svc_ai_tidas_suggestion_read(uuid, uuid)
  owner to postgres;

revoke all on function api.svc_ai_tidas_suggestion_enqueue(uuid, text, jsonb)
  from public, anon, authenticated;
revoke all on function api.svc_ai_tidas_suggestion_read(uuid, uuid)
  from public, anon, authenticated;

grant execute on function api.svc_ai_tidas_suggestion_enqueue(uuid, text, jsonb)
  to service_role;
grant execute on function api.svc_ai_tidas_suggestion_read(uuid, uuid)
  to service_role;

comment on function api.svc_ai_tidas_suggestion_enqueue(uuid, text, jsonb) is
  'Service-only AI TIDAS suggestion enqueue facade with exact v1 payload validation and active-request idempotency.';
comment on function api.svc_ai_tidas_suggestion_read(uuid, uuid) is
  'Service-only requester-scoped AI TIDAS suggestion projection; payload and internal diagnostics remain hidden.';

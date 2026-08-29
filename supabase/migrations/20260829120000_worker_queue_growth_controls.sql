-- P0 control-plane guardrails for worker write amplification and deterministic
-- request admission. All public/API signatures remain unchanged.

create or replace function private.worker_enqueue_job(
  p_job_kind text,
  p_payload_json jsonb default '{}'::jsonb,
  p_payload_schema_version text default null,
  p_subject_type text default null,
  p_subject_id uuid default null,
  p_subject_version text default null,
  p_requested_by uuid default null,
  p_requester_type text default 'user',
  p_team_id uuid default null,
  p_idempotency_key text default null,
  p_request_hash text default null,
  p_concurrency_key text default null,
  p_priority integer default null,
  p_queue_key text default null,
  p_run_after timestamptz default null,
  p_visibility text default null,
  p_max_attempts integer default null,
  p_timeout_at timestamptz default null,
  p_payload_ref jsonb default null,
  p_parent_job_id uuid default null,
  p_root_job_id uuid default null
) returns jsonb
language plpgsql
security definer
set search_path = private, api, public, util, extensions, pg_temp
as $$
declare
  v_job_kind text := lower(trim(coalesce(p_job_kind, '')));
  v_requester_type text := lower(trim(coalesce(p_requester_type, 'user')));
  v_kind private.worker_job_kinds%rowtype;
  v_existing private.worker_jobs%rowtype;
  v_job private.worker_jobs%rowtype;
  v_payload jsonb := coalesce(p_payload_json, '{}'::jsonb);
  v_payload_ref jsonb := p_payload_ref;
  v_visibility text;
  v_payload_schema_version text;
  v_priority integer;
  v_max_attempts integer;
  v_idempotency_key text := nullif(trim(p_idempotency_key), '');
  v_request_hash text := nullif(trim(p_request_hash), '');
  v_concurrency_key text := nullif(trim(p_concurrency_key), '');
  v_queue_key text := nullif(trim(p_queue_key), '');
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to enqueue worker jobs'
    );
  end if;

  select *
    into v_kind
  from private.worker_job_kinds
  where job_kind = v_job_kind;

  if v_kind.job_kind is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_KIND_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported worker job kind'
    );
  end if;

  if jsonb_typeof(v_payload) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_PAYLOAD',
      'status', 400,
      'message', 'worker job payload must be a JSON object'
    );
  end if;

  if v_payload_ref is not null and jsonb_typeof(v_payload_ref) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_PAYLOAD_REF',
      'status', 400,
      'message', 'worker job payloadRef must be a JSON object'
    );
  end if;

  if v_requester_type not in ('user', 'system', 'service', 'operator') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_REQUESTER_TYPE',
      'status', 400,
      'message', 'requesterType must be user, system, service, or operator'
    );
  end if;

  if v_requester_type = 'user' and p_requested_by is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_REQUESTED_BY_REQUIRED',
      'status', 400,
      'message', 'requestedBy is required for user-requested worker jobs'
    );
  end if;

  v_visibility := lower(trim(coalesce(p_visibility, v_kind.default_visibility)));
  if v_visibility not in ('user', 'operator', 'system') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_VISIBILITY',
      'status', 400,
      'message', 'visibility must be user, operator, or system'
    );
  end if;

  v_payload_schema_version := coalesce(
    nullif(trim(p_payload_schema_version), ''),
    v_kind.payload_schema_version
  );
  v_priority := coalesce(p_priority, v_kind.default_priority);
  v_max_attempts := greatest(1, coalesce(p_max_attempts, v_kind.default_max_attempts, 3));

  -- Scheduled maintenance uses a logical, caller-supplied idempotency key. Lock
  -- the full key for this transaction and reuse terminal as well as active jobs;
  -- an operator can still force a deliberate retry with a new explicit key.
  if v_kind.worker_queue = 'maintenance' and v_idempotency_key is not null then
    perform pg_advisory_xact_lock(hashtextextended(
      concat_ws(
        ':',
        'worker-maintenance-idempotency-v1',
        v_kind.worker_runtime,
        v_kind.job_kind,
        coalesce(p_requested_by::text, ''),
        v_idempotency_key
      ),
      0
    ));

    select *
      into v_existing
    from private.worker_jobs
    where worker_runtime = v_kind.worker_runtime
      and job_kind = v_kind.job_kind
      and requested_by is not distinct from p_requested_by
      and idempotency_key = v_idempotency_key
    order by created_at desc, id desc
    limit 1;

    if v_existing.id is not null then
      return jsonb_build_object(
        'ok', true,
        'data', private.worker_job_payload(v_existing, true),
        'reused', true
      );
    end if;
  elsif v_idempotency_key is not null then
    select *
      into v_existing
    from private.worker_jobs
    where worker_runtime = v_kind.worker_runtime
      and job_kind = v_kind.job_kind
      and requested_by is not distinct from p_requested_by
      and idempotency_key = v_idempotency_key
      and status in ('queued', 'running', 'waiting', 'stale', 'blocked')
    order by created_at desc, id desc
    limit 1;

    if v_existing.id is not null then
      return jsonb_build_object(
        'ok', true,
        'data', private.worker_job_payload(v_existing, true),
        'reused', true
      );
    end if;
  end if;

  -- The request identity deliberately excludes idempotency/concurrency keys:
  -- they are transport controls, while this identity represents the exact
  -- deterministic work request. Only the latest exact request can suppress a
  -- new enqueue, and only when it is explicitly non-retryable.
  if v_request_hash is not null then
    select *
      into v_existing
    from private.worker_jobs
    where worker_runtime = v_kind.worker_runtime
      and job_kind = v_kind.job_kind
      and requester_type = v_requester_type
      and requested_by is not distinct from p_requested_by
      and team_id is not distinct from p_team_id
      and queue_key is not distinct from v_queue_key
      and payload_schema_version = v_payload_schema_version
      and request_hash = v_request_hash
    order by created_at desc, id desc
    limit 1;

    if v_existing.status = 'failed' and v_existing.retryable is false then
      return jsonb_build_object(
        'ok', false,
        'code', 'WORKER_REQUEST_NON_RETRYABLE_FAILURE',
        'status', 409,
        'message', 'The latest exact worker request failed and is not retryable',
        'reused', true,
        'reuseReason', 'terminal_non_retryable_failure',
        'details', jsonb_build_object(
          'workerJob', private.worker_job_payload(v_existing, false)
        )
      );
    end if;
  end if;

  if v_concurrency_key is not null then
    select *
      into v_existing
    from private.worker_jobs
    where worker_runtime = v_kind.worker_runtime
      and worker_queue = v_kind.worker_queue
      and concurrency_key = v_concurrency_key
      and status in ('queued', 'running', 'waiting', 'stale')
    order by created_at desc, id desc
    limit 1;

    if v_existing.id is not null then
      return jsonb_build_object(
        'ok', false,
        'code', 'WORKER_JOB_CONCURRENCY_CONFLICT',
        'status', 409,
        'message', 'A conflicting worker job is already active',
        'details', private.worker_job_payload(v_existing, false)
      );
    end if;
  end if;

  insert into private.worker_jobs (
    job_kind,
    worker_runtime,
    worker_queue,
    priority,
    queue_key,
    root_job_id,
    parent_job_id,
    subject_type,
    subject_id,
    subject_version,
    requester_type,
    requested_by,
    team_id,
    idempotency_key,
    request_hash,
    concurrency_key,
    visibility,
    run_after,
    max_attempts,
    timeout_at,
    payload_schema_version,
    payload_json,
    payload_ref,
    result_schema_version
  ) values (
    v_kind.job_kind,
    v_kind.worker_runtime,
    v_kind.worker_queue,
    v_priority,
    v_queue_key,
    p_root_job_id,
    p_parent_job_id,
    nullif(trim(p_subject_type), ''),
    p_subject_id,
    nullif(trim(p_subject_version), ''),
    v_requester_type,
    p_requested_by,
    p_team_id,
    v_idempotency_key,
    v_request_hash,
    v_concurrency_key,
    v_visibility,
    coalesce(p_run_after, now()),
    v_max_attempts,
    p_timeout_at,
    v_payload_schema_version,
    v_payload,
    v_payload_ref,
    v_kind.result_schema_version
  )
  returning *
    into v_job;

  insert into private.worker_job_events (
    job_id,
    event_type,
    status,
    details
  ) values (
    v_job.id,
    'enqueued',
    v_job.status,
    jsonb_build_object(
      'jobKind', v_job.job_kind,
      'workerQueue', v_job.worker_queue,
      'idempotencyKey', v_job.idempotency_key,
      'concurrencyKey', v_job.concurrency_key
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', private.worker_job_payload(v_job, true),
    'reused', false
  );
exception
  when unique_violation then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_UNIQUE_CONFLICT',
      'status', 409,
      'message', 'A worker job with the same idempotency or concurrency key already exists'
    );
end;
$$;

create or replace function private.worker_heartbeat_job(
  p_job_id uuid,
  p_lease_token uuid,
  p_phase text default null,
  p_progress numeric default null,
  p_diagnostics jsonb default null,
  p_lease_seconds integer default 300
) returns jsonb
language plpgsql
security definer
set search_path = private, api, public, util, extensions, pg_temp
as $$
declare
  v_job private.worker_jobs%rowtype;
  v_lease_seconds integer := greatest(1, least(coalesce(p_lease_seconds, 300), 86400));
  v_phase text;
  v_progress numeric;
  v_diagnostics jsonb;
  v_business_changed boolean;
  v_emit_event boolean;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to heartbeat worker jobs'
    );
  end if;

  if p_progress is not null and (p_progress < 0 or p_progress > 1) then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_PROGRESS',
      'status', 400,
      'message', 'progress must be between 0 and 1'
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

  v_phase := coalesce(nullif(trim(p_phase), ''), v_job.phase);
  v_progress := coalesce(p_progress, v_job.progress);
  v_diagnostics := v_job.diagnostics || coalesce(p_diagnostics, '{}'::jsonb);
  v_business_changed := v_phase is distinct from v_job.phase
    or v_progress is distinct from v_job.progress
    or v_diagnostics is distinct from v_job.diagnostics;
  v_emit_event := v_phase is distinct from v_job.phase
    or (v_job.progress is null and p_progress is not null)
    or (
      v_job.progress is not null
      and p_progress is not null
      and floor(p_progress * 20) > floor(v_job.progress * 20)
    );

  update private.worker_jobs
    set phase = v_phase,
        progress = v_progress,
        diagnostics = v_diagnostics,
        heartbeat_at = now(),
        lease_expires_at = now() + make_interval(secs => v_lease_seconds),
        updated_at = case when v_business_changed then now() else updated_at end
  where id = v_job.id
  returning *
    into v_job;

  if v_emit_event then
    insert into private.worker_job_events (
      job_id,
      event_type,
      status,
      phase,
      progress,
      worker_id,
      lease_token,
      details
    ) values (
      v_job.id,
      'heartbeat',
      v_job.status,
      v_job.phase,
      v_job.progress,
      v_job.leased_by,
      v_job.lease_token,
      jsonb_build_object('leaseExpiresAt', v_job.lease_expires_at)
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'eventEmitted', v_emit_event,
    'data', private.worker_job_payload(v_job, true)
  );
end;
$$;

comment on function private.worker_heartbeat_job(uuid, uuid, text, numeric, jsonb, integer)
  is 'Renews a running Worker lease on every valid call. Appends a heartbeat event only for phase changes, first progress, or a crossed higher five-percent progress bucket; heartbeat diagnostics stay on the current job row and eventEmitted reports the decision.';

create or replace function api.svc_lca_cached_job_enqueue(
  p_scope text,
  p_snapshot_id uuid,
  p_request_key text,
  p_request_payload jsonb,
  p_job_kind text,
  p_job_id uuid,
  p_payload jsonb,
  p_payload_schema_version text,
  p_requested_by uuid,
  p_idempotency_key text,
  p_queue_key text default null
) returns jsonb
language plpgsql
volatile
security definer
set search_path = ''
as $$
declare
  v_scope text := nullif(btrim(p_scope), '');
  v_request_key text := nullif(btrim(p_request_key), '');
  v_cache private.lca_result_cache%rowtype;
  v_worker private.worker_jobs%rowtype;
  v_enqueue jsonb;
  v_worker_id uuid;
  v_resolved_job_id uuid;
  v_resolved_snapshot_id uuid;
  v_worker_status text;
begin
  if p_job_kind not in ('lca.solve_one', 'lca.solve_batch', 'lca.solve_all_unit', 'lca.contribution_path')
     or v_scope is null or p_snapshot_id is null or v_request_key is null
     or p_job_id is null or p_requested_by is null then
    return jsonb_build_object('ok', false, 'code', 'INVALID_LCA_JOB_REQUEST', 'status', 400);
  end if;
  perform pg_advisory_xact_lock(hashtextextended(
    'lca.cache:' || v_scope || ':' || p_snapshot_id::text || ':' || v_request_key, 0
  ));

  select * into v_cache from private.lca_result_cache
  where scope = v_scope and snapshot_id = p_snapshot_id and request_key = v_request_key
  for update;

  if v_cache.id is not null then
    update private.lca_result_cache set
      hit_count = hit_count + 1, last_accessed_at = now(), updated_at = now()
    where id = v_cache.id returning * into v_cache;
    if v_cache.status = 'ready' and v_cache.result_id is not null then
      return jsonb_build_object(
        'ok', true, 'mode', 'cache_hit', 'cache_id', v_cache.id,
        'job_id', v_cache.job_id, 'worker_job_id', v_cache.worker_job_id,
        'result_id', v_cache.result_id
      );
    end if;
    if v_cache.worker_job_id is not null then
      select * into v_worker from private.worker_jobs where id = v_cache.worker_job_id;
      if v_worker.status in ('queued', 'running', 'waiting', 'stale') then
        return jsonb_build_object(
          'ok', true, 'mode', 'in_progress', 'cache_id', v_cache.id,
          'job_id', v_cache.job_id, 'worker_job_id', v_cache.worker_job_id
        );
      end if;
      if v_worker.status = 'failed' and v_worker.retryable is false then
        return jsonb_build_object(
          'ok', false,
          'code', 'WORKER_REQUEST_NON_RETRYABLE_FAILURE',
          'status', 409,
          'mode', 'failed_cache_hit',
          'message', 'The cached worker request failed and is not retryable',
          'reused', true,
          'reuseReason', 'terminal_non_retryable_failure',
          'cache_id', v_cache.id,
          'job_id', v_cache.job_id,
          'worker_job_id', v_worker.id,
          'error_code', coalesce(v_cache.error_code, v_worker.error_code),
          'error_message', coalesce(v_cache.error_message, v_worker.error_message),
          'retryable', false,
          'details', jsonb_build_object(
            'workerJob', private.worker_job_payload(v_worker, false)
          )
        );
      end if;
    end if;
  end if;

  v_enqueue := private.worker_enqueue_job(
    p_job_kind => p_job_kind,
    p_payload_json => coalesce(p_payload, '{}'::jsonb) || jsonb_build_object('job_id', p_job_id),
    p_payload_schema_version => p_payload_schema_version,
    p_subject_type => 'lca_job',
    p_subject_id => p_job_id,
    p_subject_version => p_snapshot_id::text,
    p_requested_by => p_requested_by,
    p_requester_type => 'user',
    p_idempotency_key => p_idempotency_key,
    p_request_hash => v_request_key,
    p_concurrency_key => v_scope || ':' || p_snapshot_id::text || ':' || v_request_key,
    p_queue_key => p_queue_key,
    p_visibility => 'user'
  );
  if coalesce((v_enqueue ->> 'ok')::boolean, false) is false then return v_enqueue; end if;
  v_worker_id := (v_enqueue #>> '{data,id}')::uuid;
  v_resolved_job_id := coalesce(
    nullif(v_enqueue #>> '{data,payload,job_id}', '')::uuid,
    nullif(v_enqueue #>> '{data,subjectId}', '')::uuid,
    p_job_id
  );
  v_resolved_snapshot_id := coalesce(
    nullif(v_enqueue #>> '{data,subjectVersion}', '')::uuid,
    p_snapshot_id
  );
  v_worker_status := v_enqueue #>> '{data,status}';
  if v_resolved_snapshot_id <> p_snapshot_id then
    return jsonb_build_object('ok', false, 'code', 'LCA_JOB_IDEMPOTENCY_CONFLICT', 'status', 409);
  end if;

  insert into private.lca_result_cache as cache (
    scope, snapshot_id, request_key, request_payload, status,
    job_id, worker_job_id, hit_count, last_accessed_at, created_at, updated_at
  ) values (
    v_scope, p_snapshot_id, v_request_key, coalesce(p_request_payload, '{}'::jsonb),
    case when v_worker_status = 'blocked' then 'failed' else 'pending' end,
    v_resolved_job_id, v_worker_id, 1, now(), now(), now()
  ) on conflict (scope, snapshot_id, request_key) do update set
    request_payload = excluded.request_payload,
    status = excluded.status,
    job_id = excluded.job_id,
    worker_job_id = excluded.worker_job_id,
    result_id = null,
    error_code = null,
    error_message = null,
    hit_count = cache.hit_count + 1,
    last_accessed_at = now(),
    updated_at = now()
  returning * into v_cache;

  return jsonb_build_object(
    'ok', true,
    'mode', case
      when v_worker_status = 'blocked' then 'blocked'
      when coalesce((v_enqueue ->> 'reused')::boolean, false) then 'in_progress'
      else 'queued'
    end,
    'cache_id', v_cache.id,
    'job_id', v_cache.job_id, 'worker_job_id', v_cache.worker_job_id
  );
end;
$$;

comment on function private.worker_enqueue_job(
  text, jsonb, text, text, uuid, text, uuid, text, uuid, text, text,
  text, integer, text, timestamptz, text, integer, timestamptz, jsonb, uuid, uuid
)
  is 'Canonical service-only Worker admission. Reuses active idempotent jobs, reuses any same-key maintenance run under a transaction advisory lock, and rejects the latest exact deterministic non-retryable failure without inserting a job or event.';

-- Move review-submit coordinator state off the legacy
-- dataset_review_submit_jobs table while keeping the existing RPC contract.
--
-- The public cmd_dataset_review_submit_job_* function names and JSON field
-- names stay stable for Edge/Next compatibility. The durable coordinator rows
-- now live in public.dataset_review_submit_requests; worker_jobs remains the
-- canonical lifecycle/task fact for review_submit.submit and review_submit.gate.

create table if not exists public.dataset_review_submit_requests (
  id uuid primary key default gen_random_uuid(),
  dataset_table text not null,
  dataset_id uuid not null,
  dataset_version text not null,
  revision_checksum text not null,
  policy_profile text not null default 'review_submit_fast.v1',
  report_schema_version text not null default 'review_submit_gate_report.v1',
  status text not null default 'queued',
  requested_by uuid not null,
  gate_run_id uuid references public.dataset_review_submit_gate_runs(id) on delete set null,
  gate_worker_job_id uuid references public.worker_jobs(id) on delete set null,
  submit_worker_job_id uuid references public.worker_jobs(id) on delete set null,
  attempt_count integer not null default 0,
  last_error_code text,
  last_error_message text,
  last_error_details jsonb,
  result jsonb,
  created_at timestamptz not null default now(),
  modified_at timestamptz not null default now(),
  completed_at timestamptz,
  constraint dataset_review_submit_requests_table_check
    check (dataset_table in ('processes')),
  constraint dataset_review_submit_requests_checksum_check
    check (revision_checksum ~ '^[a-f0-9]{64}$'),
  constraint dataset_review_submit_requests_status_check
    check (status in (
      'queued',
      'waiting_gate',
      'submitting',
      'submitted',
      'blocked',
      'stale',
      'error',
      'cancelled'
    )),
  constraint dataset_review_submit_requests_attempt_count_check
    check (attempt_count >= 0),
  constraint dataset_review_submit_requests_last_error_details_check
    check (last_error_details is null or jsonb_typeof(last_error_details) = 'object'),
  constraint dataset_review_submit_requests_result_check
    check (result is null or jsonb_typeof(result) = 'object')
);

alter table public.dataset_review_submit_requests enable row level security;

create unique index if not exists dataset_review_submit_requests_active_revision_uidx
  on public.dataset_review_submit_requests (
    dataset_table,
    dataset_id,
    dataset_version,
    revision_checksum,
    policy_profile,
    report_schema_version,
    requested_by
  )
  where status in ('queued', 'waiting_gate', 'submitting');

create index if not exists dataset_review_submit_requests_requested_by_idx
  on public.dataset_review_submit_requests (requested_by, created_at desc);

create index if not exists dataset_review_submit_requests_status_idx
  on public.dataset_review_submit_requests (status, modified_at asc, created_at asc);

create index if not exists dataset_review_submit_requests_gate_run_idx
  on public.dataset_review_submit_requests (gate_run_id)
  where gate_run_id is not null;

create index if not exists dataset_review_submit_requests_gate_worker_job_idx
  on public.dataset_review_submit_requests (gate_worker_job_id)
  where gate_worker_job_id is not null;

create index if not exists dataset_review_submit_requests_submit_worker_job_idx
  on public.dataset_review_submit_requests (submit_worker_job_id)
  where submit_worker_job_id is not null;

revoke all on public.dataset_review_submit_requests from public, anon, authenticated;
grant all on public.dataset_review_submit_requests to service_role;

insert into public.dataset_review_submit_requests (
  id,
  dataset_table,
  dataset_id,
  dataset_version,
  revision_checksum,
  policy_profile,
  report_schema_version,
  status,
  requested_by,
  gate_run_id,
  gate_worker_job_id,
  submit_worker_job_id,
  attempt_count,
  last_error_code,
  last_error_message,
  last_error_details,
  result,
  created_at,
  modified_at,
  completed_at
)
select
  id,
  dataset_table,
  dataset_id,
  dataset_version,
  revision_checksum,
  policy_profile,
  report_schema_version,
  status,
  requested_by,
  gate_run_id,
  gate_worker_job_id,
  submit_worker_job_id,
  attempt_count,
  last_error_code,
  last_error_message,
  last_error_details,
  result,
  created_at,
  modified_at,
  completed_at
from public.dataset_review_submit_jobs
on conflict (id) do update
set dataset_table = excluded.dataset_table,
    dataset_id = excluded.dataset_id,
    dataset_version = excluded.dataset_version,
    revision_checksum = excluded.revision_checksum,
    policy_profile = excluded.policy_profile,
    report_schema_version = excluded.report_schema_version,
    status = excluded.status,
    requested_by = excluded.requested_by,
    gate_run_id = excluded.gate_run_id,
    gate_worker_job_id = excluded.gate_worker_job_id,
    submit_worker_job_id = excluded.submit_worker_job_id,
    attempt_count = excluded.attempt_count,
    last_error_code = excluded.last_error_code,
    last_error_message = excluded.last_error_message,
    last_error_details = excluded.last_error_details,
    result = excluded.result,
    created_at = excluded.created_at,
    modified_at = excluded.modified_at,
    completed_at = excluded.completed_at;

drop trigger if exists dataset_review_submit_jobs_assign_submit_worker_job_trigger
  on public.dataset_review_submit_jobs;

drop trigger if exists dataset_review_submit_jobs_sync_submit_worker_job_trigger
  on public.dataset_review_submit_jobs;

drop function if exists public.dataset_review_submit_jobs_assign_submit_worker_job();
drop function if exists public.dataset_review_submit_jobs_sync_submit_worker_job();

create or replace function public.cmd_dataset_review_submit_job_payload(
  p_job anyelement
) returns jsonb
language sql
stable
set search_path = public, pg_temp
as $$
  with job as (
    select to_jsonb(p_job) as row_json
  )
  select jsonb_strip_nulls(
    jsonb_build_object(
      'status', row_json->>'status',
      'reviewSubmitJobId', row_json->'id',
      'submitWorkerJobId', row_json->'submit_worker_job_id',
      'gateRunId', row_json->'gate_run_id',
      'gateWorkerJobId', row_json->'gate_worker_job_id',
      'datasetRevision', jsonb_build_object(
        'table', row_json->>'dataset_table',
        'id', row_json->'dataset_id',
        'version', row_json->>'dataset_version',
        'revisionChecksum', row_json->>'revision_checksum'
      ),
      'policy', jsonb_build_object(
        'profile', row_json->>'policy_profile',
        'reportSchemaVersion', row_json->>'report_schema_version'
      ),
      'requestedBy', row_json->'requested_by',
      'attemptCount', row_json->'attempt_count',
      'error',
        case
          when row_json->>'last_error_code' is null
            and row_json->>'last_error_message' is null
            and row_json->'last_error_details' is null then null
          else jsonb_strip_nulls(
            jsonb_build_object(
              'code', row_json->>'last_error_code',
              'message', row_json->>'last_error_message',
              'details', row_json->'last_error_details'
            )
          )
        end,
      'result', row_json->'result',
      'submitWorkerJob',
        (
          select public.worker_job_payload(w, false)
          from public.worker_jobs as w
          where w.id = nullif(row_json->>'submit_worker_job_id', '')::uuid
        ),
      'gate',
        (
          select public.cmd_dataset_review_submit_gate_payload(g)
          from public.dataset_review_submit_gate_runs as g
          where g.id = nullif(row_json->>'gate_run_id', '')::uuid
        ),
      'gateWorkerJob',
        (
          select public.worker_job_payload(w, false)
          from public.worker_jobs as w
          where w.id = nullif(row_json->>'gate_worker_job_id', '')::uuid
        ),
      'createdAt', row_json->'created_at',
      'modifiedAt', row_json->'modified_at',
      'completedAt', row_json->'completed_at'
    )
  )
  from job
$$;

create or replace function public.dataset_review_submit_requests_assign_submit_worker_job()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_kind public.worker_job_kinds%rowtype;
  v_existing public.worker_jobs%rowtype;
  v_worker_job public.worker_jobs%rowtype;
  v_payload jsonb;
  v_idempotency_key text;
  v_concurrency_key text;
begin
  if new.submit_worker_job_id is not null then
    return new;
  end if;

  select *
    into v_kind
  from public.worker_job_kinds
  where job_kind = 'review_submit.submit';

  if v_kind.job_kind is null then
    raise exception
      using
        errcode = 'P0001',
        message = 'review_submit.submit worker job kind is not registered';
  end if;

  v_payload := jsonb_build_object(
    'datasetRevision', jsonb_build_object(
      'table', new.dataset_table,
      'id', new.dataset_id,
      'version', new.dataset_version,
      'revisionChecksum', new.revision_checksum
    ),
    'policy', jsonb_build_object(
      'profile', new.policy_profile,
      'reportSchemaVersion', new.report_schema_version
    ),
    'requestedBy', new.requested_by,
    'reviewSubmitJobId', new.id
  );
  v_idempotency_key := concat_ws(
    ':',
    'review_submit.submit',
    new.dataset_table,
    new.dataset_id::text,
    new.dataset_version,
    new.revision_checksum,
    new.policy_profile,
    new.report_schema_version,
    new.requested_by::text
  );
  v_concurrency_key := concat_ws(
    ':',
    'review_submit.submit',
    new.dataset_table,
    new.dataset_id::text,
    new.dataset_version,
    new.requested_by::text
  );

  select *
    into v_existing
  from public.worker_jobs
  where worker_runtime = v_kind.worker_runtime
    and job_kind = v_kind.job_kind
    and requested_by is not distinct from new.requested_by
    and idempotency_key = v_idempotency_key
    and status in ('queued', 'running', 'waiting', 'stale', 'blocked')
  order by created_at desc
  limit 1
  for update;

  if v_existing.id is not null then
    new.submit_worker_job_id := v_existing.id;
    return new;
  end if;

  select *
    into v_existing
  from public.worker_jobs
  where worker_runtime = v_kind.worker_runtime
    and worker_queue = v_kind.worker_queue
    and concurrency_key = v_concurrency_key
    and status in ('queued', 'running', 'waiting', 'stale')
  order by created_at desc
  limit 1
  for update;

  if v_existing.id is not null then
    raise exception
      using
        errcode = 'P0001',
        message = 'conflicting active review-submit root worker job exists',
        detail = jsonb_build_object(
          'existingWorkerJobId', v_existing.id,
          'concurrencyKey', v_concurrency_key
        )::text;
  end if;

  insert into public.worker_jobs (
    job_kind,
    worker_runtime,
    worker_queue,
    priority,
    subject_type,
    subject_id,
    subject_version,
    requester_type,
    requested_by,
    idempotency_key,
    request_hash,
    concurrency_key,
    status,
    phase,
    progress,
    visibility,
    max_attempts,
    payload_schema_version,
    payload_json,
    result_schema_version,
    result_ref
  ) values (
    v_kind.job_kind,
    v_kind.worker_runtime,
    v_kind.worker_queue,
    v_kind.default_priority,
    new.dataset_table,
    new.dataset_id,
    new.dataset_version,
    'user',
    new.requested_by,
    v_idempotency_key,
    new.revision_checksum,
    v_concurrency_key,
    'waiting',
    new.status,
    0,
    v_kind.default_visibility,
    greatest(1, v_kind.default_max_attempts),
    v_kind.payload_schema_version,
    v_payload,
    v_kind.result_schema_version,
    jsonb_build_object(
      'domainSource', 'dataset_review_submit_requests',
      'reviewSubmitJobId', new.id
    )
  )
  returning *
    into v_worker_job;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    phase,
    progress,
    details
  ) values (
    v_worker_job.id,
    'enqueued',
    v_worker_job.status,
    v_worker_job.phase,
    v_worker_job.progress,
    jsonb_build_object(
      'jobKind', v_worker_job.job_kind,
      'workerQueue', v_worker_job.worker_queue,
      'idempotencyKey', v_worker_job.idempotency_key,
      'concurrencyKey', v_worker_job.concurrency_key,
      'source', 'dataset_review_submit_requests_assign_submit_worker_job'
    )
  );

  new.submit_worker_job_id := v_worker_job.id;
  return new;
end;
$$;

create or replace function public.dataset_review_submit_requests_sync_submit_worker_job()
returns trigger
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_worker_status text;
  v_progress numeric;
  v_result_json jsonb;
  v_result_ref jsonb;
  v_error_code text;
  v_error_message text;
  v_error_details jsonb;
  v_blocker_codes text[];
  v_resolution_scope text;
begin
  if new.submit_worker_job_id is null then
    return new;
  end if;

  v_worker_status := case
    when new.status in ('queued', 'waiting_gate', 'submitting') then 'waiting'
    when new.status = 'submitted' then 'completed'
    when new.status = 'blocked' then 'blocked'
    when new.status = 'stale' then 'stale'
    when new.status = 'cancelled' then 'cancelled'
    else 'failed'
  end;

  v_progress := case
    when new.status = 'queued' then 0
    when new.status = 'waiting_gate' then 0.25
    when new.status = 'submitting' then 0.75
    when new.status = 'submitted' then 1
    else null
  end;

  v_result_ref := jsonb_build_object(
    'domainSource', 'dataset_review_submit_requests',
    'reviewSubmitJobId', new.id
  );

  v_result_json := case
    when new.status = 'submitted' then jsonb_strip_nulls(
      jsonb_build_object(
        'status', 'submitted',
        'reviewSubmitJobId', new.id,
        'datasetRevision', jsonb_build_object(
          'table', new.dataset_table,
          'id', new.dataset_id,
          'version', new.dataset_version,
          'revisionChecksum', new.revision_checksum
        ),
        'result', new.result
      )
    )
    else null
  end;

  v_error_code := case
    when v_worker_status in ('blocked', 'stale', 'failed', 'cancelled') then
      coalesce(new.last_error_code, 'REVIEW_SUBMIT_JOB_' || upper(new.status))
    else null
  end;
  v_error_message := case
    when v_worker_status in ('blocked', 'stale', 'failed', 'cancelled') then
      coalesce(new.last_error_message, 'Review-submit job status is ' || new.status)
    else null
  end;
  v_error_details := case
    when v_worker_status in ('blocked', 'stale', 'failed', 'cancelled') then
      coalesce(new.last_error_details, '{}'::jsonb)
    else null
  end;
  v_blocker_codes := case
    when v_worker_status = 'blocked' then array[coalesce(new.last_error_code, 'REVIEW_SUBMIT_GATE_BLOCKED')]
    else '{}'::text[]
  end;
  v_resolution_scope := case
    when v_worker_status = 'blocked' then 'user'
    else null
  end;

  update public.worker_jobs
    set status = v_worker_status,
        phase = new.status,
        progress = v_progress,
        result_json = case
          when v_worker_status = 'completed' then v_result_json
          else result_json
        end,
        result_ref = coalesce(result_ref, '{}'::jsonb) || v_result_ref,
        error_code = v_error_code,
        error_message = v_error_message,
        error_details = v_error_details,
        blocker_codes = v_blocker_codes,
        resolution_scope = v_resolution_scope,
        retryable = case
          when v_worker_status in ('failed', 'stale') then true
          when v_worker_status in ('completed', 'blocked', 'cancelled') then false
          else null
        end,
        updated_at = now(),
        finished_at = case
          when v_worker_status in ('completed', 'blocked', 'stale', 'failed', 'cancelled') then coalesce(finished_at, now())
          else null
        end,
        cancelled_at = case
          when v_worker_status = 'cancelled' then coalesce(cancelled_at, now())
          else cancelled_at
        end
  where id = new.submit_worker_job_id;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    phase,
    progress,
    message,
    details
  )
  select
    new.submit_worker_job_id,
    'review_submit_status_synced',
    v_worker_status,
    new.status,
    v_progress,
    v_error_message,
    jsonb_strip_nulls(
      jsonb_build_object(
        'reviewSubmitJobId', new.id,
        'errorCode', v_error_code,
        'blockerCodes', to_jsonb(v_blocker_codes),
        'resolutionScope', v_resolution_scope
      )
    );

  return new;
end;
$$;

drop trigger if exists dataset_review_submit_requests_assign_submit_worker_job_trigger
  on public.dataset_review_submit_requests;

create trigger dataset_review_submit_requests_assign_submit_worker_job_trigger
  before insert or update of submit_worker_job_id
  on public.dataset_review_submit_requests
  for each row
  execute function public.dataset_review_submit_requests_assign_submit_worker_job();

drop trigger if exists dataset_review_submit_requests_sync_submit_worker_job_trigger
  on public.dataset_review_submit_requests;

create trigger dataset_review_submit_requests_sync_submit_worker_job_trigger
  after insert or update of
    status,
    last_error_code,
    last_error_message,
    last_error_details,
    result,
    submit_worker_job_id
  on public.dataset_review_submit_requests
  for each row
  execute function public.dataset_review_submit_requests_sync_submit_worker_job();

update public.worker_jobs as worker_job
set result_ref = coalesce(worker_job.result_ref, '{}'::jsonb) || jsonb_build_object(
      'domainSource', 'dataset_review_submit_requests',
      'reviewSubmitJobId', request.id
    ),
    updated_at = now()
from public.dataset_review_submit_requests as request
where worker_job.id = request.submit_worker_job_id
  and worker_job.job_kind = 'review_submit.submit';

create or replace function public.cmd_dataset_review_submit_job_enqueue(
  p_table text,
  p_id uuid,
  p_version text,
  p_revision_checksum text,
  p_policy_profile text default 'review_submit_fast.v1',
  p_report_schema_version text default 'review_submit_gate_report.v1',
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_dataset_row jsonb;
  v_owner_id uuid;
  v_state_code integer;
  v_kind public.worker_job_kinds%rowtype;
  v_worker_existing public.worker_jobs%rowtype;
  v_worker_job public.worker_jobs%rowtype;
  v_worker_payload jsonb;
  v_idempotency_key text;
  v_concurrency_key text;
  v_worker_status text;
  v_job_status text := 'waiting_gate';
  v_last_error_code text;
  v_last_error_message text;
  v_last_error_details jsonb;
  v_completed_at timestamptz;
  v_existing public.dataset_review_submit_requests%rowtype;
  v_job public.dataset_review_submit_requests%rowtype;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table <> 'processes' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Review-submit jobs currently support process datasets only'
    );
  end if;

  if coalesce(p_revision_checksum, '') !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVISION_CHECKSUM_REQUIRED',
      'status', 400,
      'message', 'revisionChecksum must be a lowercase SHA-256 hex digest'
    );
  end if;

  if coalesce(p_policy_profile, '') <> 'review_submit_fast.v1' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_POLICY_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported review-submit gate policy profile',
      'details', jsonb_build_object('policy_profile', p_policy_profile)
    );
  end if;

  if coalesce(p_report_schema_version, '') <> 'review_submit_gate_report.v1' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_GATE_SCHEMA_UNSUPPORTED',
      'status', 400,
      'message', 'Unsupported review-submit gate report schema version',
      'details', jsonb_build_object('report_schema_version', p_report_schema_version)
    );
  end if;

  v_dataset_row := public.cmd_review_get_dataset_row(p_table, p_id, p_version, false);

  if v_dataset_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  v_owner_id := nullif(v_dataset_row->>'user_id', '')::uuid;
  v_state_code := coalesce(nullif(v_dataset_row->>'state_code', '')::integer, 0);

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can enqueue review submission'
    );
  end if;

  if v_state_code >= 100 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_ALREADY_PUBLISHED',
      'status', 409,
      'message', 'Published datasets cannot be submitted for review again'
    );
  end if;

  if v_state_code >= 20 then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_UNDER_REVIEW',
      'status', 409,
      'message', 'Dataset is already under review'
    );
  end if;

  perform pg_advisory_xact_lock(
    hashtextextended(
      concat_ws(
        ':',
        'review_submit_request',
        p_table,
        p_id::text,
        p_version,
        p_revision_checksum,
        p_policy_profile,
        p_report_schema_version,
        v_actor::text
      ),
      0
    )
  );

  select *
    into v_existing
  from public.dataset_review_submit_requests
  where dataset_table = p_table
    and dataset_id = p_id
    and dataset_version = p_version
    and revision_checksum = p_revision_checksum
    and policy_profile = p_policy_profile
    and report_schema_version = p_report_schema_version
    and requested_by = v_actor
    and status in ('queued', 'waiting_gate', 'submitting', 'submitted')
  order by created_at desc
  limit 1
  for update;

  if v_existing.id is not null then
    return jsonb_build_object(
      'ok', true,
      'data', public.cmd_dataset_review_submit_job_payload(v_existing)
    );
  end if;

  select *
    into v_kind
  from public.worker_job_kinds
  where job_kind = 'review_submit.gate';

  if v_kind.job_kind is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_KIND_UNSUPPORTED',
      'status', 500,
      'message', 'review_submit.gate worker job kind is not registered'
    );
  end if;

  v_worker_payload := jsonb_build_object(
    'datasetRevision', jsonb_build_object(
      'table', p_table,
      'id', p_id,
      'version', p_version,
      'revisionChecksum', p_revision_checksum
    ),
    'policy', jsonb_build_object(
      'profile', p_policy_profile,
      'reportSchemaVersion', p_report_schema_version
    ),
    'requestedBy', v_actor
  );
  v_idempotency_key := concat_ws(
    ':',
    'review_submit.gate',
    p_table,
    p_id::text,
    p_version,
    p_revision_checksum,
    p_policy_profile,
    p_report_schema_version,
    v_actor::text
  );
  v_concurrency_key := concat_ws(
    ':',
    'review_submit.gate',
    p_table,
    p_id::text,
    p_version,
    v_actor::text
  );

  select *
    into v_worker_existing
  from public.worker_jobs
  where worker_runtime = 'calculator'
    and job_kind = 'review_submit.gate'
    and requested_by is not distinct from v_actor
    and idempotency_key = v_idempotency_key
    and status in ('queued', 'running', 'waiting', 'stale', 'blocked')
  order by created_at desc
  limit 1
  for update;

  if v_worker_existing.id is not null then
    v_worker_job := v_worker_existing;
  else
    select *
      into v_worker_existing
    from public.worker_jobs
    where worker_runtime = 'calculator'
      and worker_queue = 'review_submit_gate'
      and concurrency_key = v_concurrency_key
      and status in ('queued', 'running', 'waiting', 'stale')
    order by created_at desc
    limit 1
    for update;

    if v_worker_existing.id is not null then
      return jsonb_build_object(
        'ok', false,
        'code', 'WORKER_JOB_CONCURRENCY_CONFLICT',
        'status', 409,
        'message', 'A conflicting review-submit gate job is already active',
        'details', public.worker_job_payload(v_worker_existing, false)
      );
    end if;

    insert into public.worker_jobs (
      job_kind,
      worker_runtime,
      worker_queue,
      priority,
      subject_type,
      subject_id,
      subject_version,
      requester_type,
      requested_by,
      idempotency_key,
      concurrency_key,
      visibility,
      max_attempts,
      payload_schema_version,
      payload_json,
      result_schema_version
    ) values (
      v_kind.job_kind,
      v_kind.worker_runtime,
      v_kind.worker_queue,
      v_kind.default_priority,
      p_table,
      p_id,
      p_version,
      'user',
      v_actor,
      v_idempotency_key,
      v_concurrency_key,
      v_kind.default_visibility,
      v_kind.default_max_attempts,
      v_kind.payload_schema_version,
      v_worker_payload,
      v_kind.result_schema_version
    )
    returning *
      into v_worker_job;

    insert into public.worker_job_events (
      job_id,
      event_type,
      status,
      details
    ) values (
      v_worker_job.id,
      'enqueued',
      v_worker_job.status,
      jsonb_build_object(
        'jobKind', v_worker_job.job_kind,
        'workerQueue', v_worker_job.worker_queue,
        'idempotencyKey', v_worker_job.idempotency_key,
        'concurrencyKey', v_worker_job.concurrency_key,
        'source', 'cmd_dataset_review_submit_job_enqueue'
      )
    );
  end if;

  v_worker_status := v_worker_job.status;
  v_job_status := case
    when v_worker_status = 'completed' then 'queued'
    when v_worker_status in ('queued', 'running', 'waiting', 'stale') then 'waiting_gate'
    when v_worker_status = 'blocked' then 'blocked'
    when v_worker_status = 'cancelled' then 'cancelled'
    else 'error'
  end;

  if v_job_status = 'blocked' then
    v_last_error_code := 'REVIEW_SUBMIT_GATE_BLOCKED';
    v_last_error_message := 'Review-submit gate blocked this dataset revision';
    v_last_error_details := public.worker_job_payload(v_worker_job, false);
  elsif v_job_status = 'stale' then
    v_last_error_code := 'REVIEW_SUBMIT_GATE_STALE';
    v_last_error_message := 'Review-submit gate run is stale for the submitted dataset revision';
    v_last_error_details := public.worker_job_payload(v_worker_job, false);
  elsif v_job_status = 'error' then
    v_last_error_code := coalesce(v_worker_job.error_code, 'REVIEW_SUBMIT_GATE_ERROR');
    v_last_error_message := coalesce(v_worker_job.error_message, 'Review-submit gate failed before review submission');
    v_last_error_details := public.worker_job_payload(v_worker_job, false);
  elsif v_job_status = 'cancelled' then
    v_last_error_code := 'REVIEW_SUBMIT_JOB_CANCELLED';
    v_last_error_message := 'Review-submit job was cancelled';
    v_last_error_details := public.worker_job_payload(v_worker_job, false);
  end if;

  if v_job_status in ('submitted', 'blocked', 'stale', 'error', 'cancelled') then
    v_completed_at := now();
  end if;

  insert into public.dataset_review_submit_requests (
    dataset_table,
    dataset_id,
    dataset_version,
    revision_checksum,
    policy_profile,
    report_schema_version,
    status,
    requested_by,
    gate_worker_job_id,
    last_error_code,
    last_error_message,
    last_error_details,
    completed_at
  )
  values (
    p_table,
    p_id,
    p_version,
    p_revision_checksum,
    p_policy_profile,
    p_report_schema_version,
    v_job_status,
    v_actor,
    v_worker_job.id,
    v_last_error_code,
    v_last_error_message,
    v_last_error_details,
    v_completed_at
  )
  returning *
    into v_job;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_review_submit_job_enqueue',
    v_actor,
    p_table,
    p_id,
    p_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'review_submit_job_id', v_job.id,
      'review_submit_request_id', v_job.id,
      'gate_worker_job_id', v_worker_job.id,
      'gate_worker_job_status', v_worker_status,
      'revision_checksum', p_revision_checksum,
      'policy_profile', p_policy_profile,
      'report_schema_version', p_report_schema_version
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_job_payload(v_job)
  );
end;
$$;

create or replace function public.cmd_dataset_review_submit_job_read(
  p_job_id uuid
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_is_service boolean := coalesce(util.is_service_request(), false);
  v_job public.dataset_review_submit_requests%rowtype;
begin
  if p_job_id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_ID_REQUIRED',
      'status', 400,
      'message', 'reviewSubmitJobId is required'
    );
  end if;

  if v_actor is null and not v_is_service then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  select *
    into v_job
  from public.dataset_review_submit_requests
  where id = p_job_id;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit job not found'
    );
  end if;

  if not v_is_service and v_job.requested_by is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the job requester can read this review-submit job'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_job_payload(v_job)
  );
end;
$$;

create or replace function public.cmd_dataset_review_submit_job_read_latest(
  p_table text,
  p_id uuid,
  p_version text,
  p_revision_checksum text default null
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_actor uuid := auth.uid();
  v_dataset_row jsonb;
  v_owner_id uuid;
  v_job public.dataset_review_submit_requests%rowtype;
begin
  if v_actor is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'AUTH_REQUIRED',
      'status', 401,
      'message', 'Authentication required'
    );
  end if;

  if p_table <> 'processes' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_DATASET_TABLE',
      'status', 400,
      'message', 'Review-submit jobs currently support process datasets only'
    );
  end if;

  if p_revision_checksum is not null
    and p_revision_checksum !~ '^[a-f0-9]{64}$' then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVISION_CHECKSUM_REQUIRED',
      'status', 400,
      'message', 'revisionChecksum must be a lowercase SHA-256 hex digest'
    );
  end if;

  v_dataset_row := public.cmd_review_get_dataset_row(p_table, p_id, p_version, false);

  if v_dataset_row is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found'
    );
  end if;

  v_owner_id := nullif(v_dataset_row->>'user_id', '')::uuid;

  if v_owner_id is distinct from v_actor then
    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the dataset owner can read review-submit jobs'
    );
  end if;

  select *
    into v_job
  from public.dataset_review_submit_requests
  where dataset_table = p_table
    and dataset_id = p_id
    and dataset_version = p_version
    and requested_by = v_actor
    and (p_revision_checksum is null or revision_checksum = p_revision_checksum)
  order by created_at desc
  limit 1;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit job not found'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_job_payload(v_job)
  );
end;
$$;

create or replace function public.cmd_dataset_review_submit_job_claim(
  p_qty integer default 10,
  p_stale_submitting_seconds integer default 300
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_qty integer := greatest(1, least(coalesce(p_qty, 10), 50));
  v_stale_seconds integer := greatest(30, coalesce(p_stale_submitting_seconds, 300));
  v_jobs jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to claim review-submit jobs'
    );
  end if;

  with claimed as (
    select id
    from public.dataset_review_submit_requests
    where status in ('queued', 'waiting_gate')
      or (
        status = 'submitting'
        and modified_at < now() - make_interval(secs => v_stale_seconds)
      )
    order by
      case status
        when 'queued' then 0
        when 'waiting_gate' then 1
        else 2
      end,
      created_at asc
    for update skip locked
    limit v_qty
  ),
  updated as (
    update public.dataset_review_submit_requests as request
      set status = 'submitting',
          attempt_count = request.attempt_count + 1,
          modified_at = now()
    from claimed
    where request.id = claimed.id
    returning request.*
  )
  select coalesce(jsonb_agg(public.cmd_dataset_review_submit_job_payload(updated)), '[]'::jsonb)
    into v_jobs
  from updated;

  return jsonb_build_object(
    'ok', true,
    'data', v_jobs
  );
end;
$$;

create or replace function public.cmd_dataset_review_submit_job_record_result(
  p_job_id uuid,
  p_status text,
  p_gate_run_id uuid default null,
  p_result jsonb default null,
  p_error_code text default null,
  p_error_message text default null,
  p_error_details jsonb default null,
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_status text := lower(trim(coalesce(p_status, '')));
  v_job public.dataset_review_submit_requests%rowtype;
  v_gate public.dataset_review_submit_gate_runs%rowtype;
  v_completed_at timestamptz;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to record review-submit job results'
    );
  end if;

  if v_status not in ('waiting_gate', 'blocked', 'stale', 'error', 'cancelled') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_JOB_STATUS',
      'status', 400,
      'message', 'status must be waiting_gate, blocked, stale, error, or cancelled'
    );
  end if;

  if p_result is not null and jsonb_typeof(p_result) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_JOB_RESULT',
      'status', 400,
      'message', 'result must be a JSON object'
    );
  end if;

  if p_error_details is not null and jsonb_typeof(p_error_details) <> 'object' then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_REVIEW_SUBMIT_JOB_ERROR_DETAILS',
      'status', 400,
      'message', 'error details must be a JSON object'
    );
  end if;

  select *
    into v_job
  from public.dataset_review_submit_requests
  where id = p_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit job not found'
    );
  end if;

  if p_gate_run_id is not null then
    select *
      into v_gate
    from public.dataset_review_submit_gate_runs
    where id = p_gate_run_id;

    if v_gate.id is null then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate run not found'
      );
    end if;

    if v_gate.dataset_table <> v_job.dataset_table
      or v_gate.dataset_id <> v_job.dataset_id
      or v_gate.dataset_version <> v_job.dataset_version
      or v_gate.revision_checksum <> v_job.revision_checksum
      or v_gate.policy_profile <> v_job.policy_profile
      or v_gate.report_schema_version <> v_job.report_schema_version then
      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate run does not match this review-submit job'
      );
    end if;
  end if;

  if v_status in ('blocked', 'stale', 'error', 'cancelled') then
    v_completed_at := now();
  end if;

  update public.dataset_review_submit_requests
    set status = v_status,
        gate_run_id = coalesce(p_gate_run_id, gate_run_id),
        result = p_result,
        last_error_code =
          case when v_status = 'waiting_gate' then null else p_error_code end,
        last_error_message =
          case when v_status = 'waiting_gate' then null else p_error_message end,
        last_error_details =
          case when v_status = 'waiting_gate' then null else p_error_details end,
        modified_at = now(),
        completed_at = v_completed_at
  where id = v_job.id
  returning *
    into v_job;

  insert into public.command_audit_log (
    command,
    actor_user_id,
    target_table,
    target_id,
    target_version,
    payload
  )
  values (
    'cmd_dataset_review_submit_job_record_result',
    v_job.requested_by,
    v_job.dataset_table,
    v_job.dataset_id,
    v_job.dataset_version,
    coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
      'review_submit_job_id', v_job.id,
      'review_submit_request_id', v_job.id,
      'gate_run_id', v_job.gate_run_id,
      'status', v_status,
      'error_code', p_error_code
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.cmd_dataset_review_submit_job_payload(v_job)
  );
end;
$$;

create or replace function public.cmd_review_submit_from_job(
  p_job_id uuid,
  p_audit jsonb default '{}'::jsonb
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_job public.dataset_review_submit_requests%rowtype;
  v_worker_job public.worker_jobs%rowtype;
  v_worker_result_checksum text;
  v_dataset_found boolean;
  v_owner_id uuid;
  v_state_code integer;
  v_modified_at timestamptz;
  v_submit_result jsonb;
  v_error_code text;
  v_error_status integer;
  v_error_message text;
  v_job_status text;
  v_prev_sub text;
  v_prev_role text;
  v_prev_claims text;
  v_submit_audit jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to submit review from a job'
    );
  end if;

  select *
    into v_job
  from public.dataset_review_submit_requests
  where id = p_job_id
  for update;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Review-submit job not found'
    );
  end if;

  if v_job.status = 'submitted' then
    return jsonb_build_object(
      'ok', true,
      'data', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_job.status in ('blocked', 'stale', 'error', 'cancelled') then
    return jsonb_build_object(
      'ok', false,
      'code', coalesce(v_job.last_error_code, 'REVIEW_SUBMIT_JOB_NOT_ACTIVE'),
      'status', 409,
      'message', coalesce(v_job.last_error_message, 'Review-submit job is not active'),
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_job.gate_worker_job_id is null and v_job.gate_run_id is null then
    update public.dataset_review_submit_requests
      set status = 'error',
          last_error_code = 'REVIEW_SUBMIT_JOB_GATE_REQUIRED',
          last_error_message = 'Review-submit job is missing a gate job',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_GATE_REQUIRED',
      'status', 409,
      'message', 'Review-submit job is missing a gate job',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_job.gate_worker_job_id is not null then
    select *
      into v_worker_job
    from public.worker_jobs
    where id = v_job.gate_worker_job_id
    for update;

    if v_worker_job.id is null then
      update public.dataset_review_submit_requests
        set status = 'error',
            last_error_code = 'REVIEW_SUBMIT_GATE_NOT_FOUND',
            last_error_message = 'Review-submit gate worker job not found',
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_FOUND',
        'status', 404,
        'message', 'Review-submit gate worker job not found',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.job_kind <> 'review_submit.gate'
      or v_worker_job.subject_type <> v_job.dataset_table
      or v_worker_job.subject_id <> v_job.dataset_id
      or v_worker_job.subject_version <> v_job.dataset_version
      or v_worker_job.requested_by is distinct from v_job.requested_by
      or v_worker_job.payload_json #>> '{policy,profile}' is distinct from v_job.policy_profile
      or v_worker_job.payload_json #>> '{policy,reportSchemaVersion}' is distinct from v_job.report_schema_version then
      update public.dataset_review_submit_requests
        set status = 'stale',
            last_error_code = 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
            last_error_message = 'Review-submit gate worker job does not match this review-submit job',
            last_error_details = jsonb_build_object(
              'gateWorkerJobId', v_worker_job.id,
              'jobKind', v_worker_job.job_kind,
              'subjectType', v_worker_job.subject_type,
              'subjectId', v_worker_job.subject_id,
              'subjectVersion', v_worker_job.subject_version,
              'requestedBy', v_worker_job.requested_by,
              'policyProfile', v_worker_job.payload_json #>> '{policy,profile}',
              'reportSchemaVersion', v_worker_job.payload_json #>> '{policy,reportSchemaVersion}'
            ),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate worker job does not match this review-submit job',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.status in ('queued', 'running', 'waiting', 'stale') then
      update public.dataset_review_submit_requests
        set status = 'waiting_gate',
            last_error_code = null,
            last_error_message = null,
            last_error_details = null,
            modified_at = now(),
            completed_at = null
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_NOT_READY',
        'status', 409,
        'message', 'Review-submit gate has not passed yet',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.status = 'blocked' then
      update public.dataset_review_submit_requests
        set status = 'blocked',
            last_error_code = 'REVIEW_SUBMIT_GATE_BLOCKED',
            last_error_message = 'Review-submit gate blocked this dataset revision',
            last_error_details = public.worker_job_payload(v_worker_job, false),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_BLOCKED',
        'status', 409,
        'message', 'Review-submit gate blocked this dataset revision',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.status = 'cancelled' then
      update public.dataset_review_submit_requests
        set status = 'cancelled',
            last_error_code = 'REVIEW_SUBMIT_JOB_CANCELLED',
            last_error_message = 'Review-submit gate worker job was cancelled',
            last_error_details = public.worker_job_payload(v_worker_job, false),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_JOB_CANCELLED',
        'status', 409,
        'message', 'Review-submit gate worker job was cancelled',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.status <> 'completed' then
      update public.dataset_review_submit_requests
        set status = 'error',
            last_error_code = coalesce(v_worker_job.error_code, 'REVIEW_SUBMIT_GATE_ERROR'),
            last_error_message = coalesce(v_worker_job.error_message, 'Review-submit gate failed before review submission'),
            last_error_details = public.worker_job_payload(v_worker_job, false),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', coalesce(v_worker_job.error_code, 'REVIEW_SUBMIT_GATE_ERROR'),
        'status', 502,
        'message', coalesce(v_worker_job.error_message, 'Review-submit gate failed before review submission'),
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if coalesce(v_worker_job.result_json->>'status', '') <> 'passed' then
      update public.dataset_review_submit_requests
        set status = 'error',
            last_error_code = 'REVIEW_SUBMIT_GATE_ERROR',
            last_error_message = 'Review-submit gate worker job completed without a passed result',
            last_error_details = public.worker_job_payload(v_worker_job, false),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_ERROR',
        'status', 502,
        'message', 'Review-submit gate worker job completed without a passed result',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    if v_worker_job.result_json #>> '{datasetRevision,table}' is distinct from v_job.dataset_table
      or v_worker_job.result_json #>> '{datasetRevision,id}' is distinct from v_job.dataset_id::text
      or v_worker_job.result_json #>> '{datasetRevision,version}' is distinct from v_job.dataset_version then
      update public.dataset_review_submit_requests
        set status = 'stale',
            last_error_code = 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
            last_error_message = 'Review-submit gate worker result does not match this review-submit job',
            last_error_details = jsonb_build_object(
              'gateWorkerJobId', v_worker_job.id,
              'resultDatasetRevision', v_worker_job.result_json->'datasetRevision'
            ),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_DATASET_MISMATCH',
        'status', 409,
        'message', 'Review-submit gate worker result does not match this review-submit job',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;

    v_worker_result_checksum := v_worker_job.result_json #>> '{datasetRevision,revisionChecksum}';

    if v_worker_result_checksum is distinct from v_job.revision_checksum then
      update public.dataset_review_submit_requests
        set status = 'stale',
            last_error_code = 'REVIEW_SUBMIT_GATE_STALE',
            last_error_message = 'Review-submit gate worker job is stale for the submitted dataset revision',
            last_error_details = jsonb_build_object(
              'gateWorkerJobId', v_worker_job.id,
              'expectedRevisionChecksum', v_job.revision_checksum,
              'actualRevisionChecksum', v_worker_result_checksum
            ),
            modified_at = now(),
            completed_at = now()
      where id = v_job.id
      returning *
        into v_job;

      return jsonb_build_object(
        'ok', false,
        'code', 'REVIEW_SUBMIT_GATE_STALE',
        'status', 409,
        'message', 'Review-submit gate worker job is stale for the submitted dataset revision',
        'details', public.cmd_dataset_review_submit_job_payload(v_job)
      );
    end if;
  end if;

  execute format(
    'select true, user_id, state_code, modified_at from public.%I where id = $1 and version = $2',
    v_job.dataset_table
  )
    into v_dataset_found, v_owner_id, v_state_code, v_modified_at
    using v_job.dataset_id, v_job.dataset_version;

  if coalesce(v_dataset_found, false) is false then
    update public.dataset_review_submit_requests
      set status = 'error',
          last_error_code = 'DATASET_NOT_FOUND',
          last_error_message = 'Dataset not found',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_NOT_FOUND',
      'status', 404,
      'message', 'Dataset not found',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_owner_id is distinct from v_job.requested_by then
    update public.dataset_review_submit_requests
      set status = 'error',
          last_error_code = 'DATASET_OWNER_REQUIRED',
          last_error_message = 'Only the job requester can submit this dataset for review',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'DATASET_OWNER_REQUIRED',
      'status', 403,
      'message', 'Only the job requester can submit this dataset for review',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if coalesce(v_state_code, 0) >= 100 then
    update public.dataset_review_submit_requests
      set status = 'error',
          last_error_code = 'DATA_ALREADY_PUBLISHED',
          last_error_message = 'Published datasets cannot be submitted for review again',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_ALREADY_PUBLISHED',
      'status', 409,
      'message', 'Published datasets cannot be submitted for review again',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if coalesce(v_state_code, 0) >= 20 then
    update public.dataset_review_submit_requests
      set status = 'error',
          last_error_code = 'DATA_UNDER_REVIEW',
          last_error_message = 'Dataset is already under review',
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'DATA_UNDER_REVIEW',
      'status', 409,
      'message', 'Dataset is already under review',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  if v_modified_at > v_job.created_at then
    update public.dataset_review_submit_requests
      set status = 'stale',
          last_error_code = 'REVIEW_SUBMIT_JOB_STALE',
          last_error_message = 'Dataset changed after this review-submit job was created',
          last_error_details = jsonb_build_object(
            'jobCreatedAt', to_jsonb(v_job.created_at),
            'datasetModifiedAt', to_jsonb(v_modified_at)
          ),
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    return jsonb_build_object(
      'ok', false,
      'code', 'REVIEW_SUBMIT_JOB_STALE',
      'status', 409,
      'message', 'Dataset changed after this review-submit job was created',
      'details', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  v_prev_sub := current_setting('request.jwt.claim.sub', true);
  v_prev_role := current_setting('request.jwt.claim.role', true);
  v_prev_claims := current_setting('request.jwt.claims', true);

  perform set_config('request.jwt.claim.sub', v_job.requested_by::text, true);
  perform set_config('request.jwt.claim.role', 'authenticated', true);
  perform set_config(
    'request.jwt.claims',
    jsonb_build_object(
      'sub', v_job.requested_by::text,
      'role', 'authenticated'
    )::text,
    true
  );

  v_submit_audit := coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
    'source', 'cmd_review_submit_from_job',
    'review_submit_job_id', v_job.id,
    'review_submit_request_id', v_job.id,
    'review_submit_gate_worker_job_id', v_job.gate_worker_job_id
  );

  if v_job.gate_worker_job_id is not null then
    v_submit_result := public.cmd_review_submit_without_gate(
      v_job.dataset_table,
      v_job.dataset_id,
      v_job.dataset_version,
      v_submit_audit || jsonb_build_object(
        'review_submit_revision_checksum', v_job.revision_checksum,
        'review_submit_policy_profile', v_job.policy_profile,
        'review_submit_report_schema_version', v_job.report_schema_version
      )
    );
  else
    v_submit_result := public.cmd_review_submit(
      p_table => v_job.dataset_table,
      p_id => v_job.dataset_id,
      p_version => v_job.dataset_version,
      p_audit => v_submit_audit,
      p_review_submit_gate_run_id => v_job.gate_run_id,
      p_review_submit_revision_checksum => v_job.revision_checksum,
      p_review_submit_policy_profile => v_job.policy_profile,
      p_review_submit_report_schema_version => v_job.report_schema_version
    );
  end if;

  perform set_config('request.jwt.claim.sub', coalesce(v_prev_sub, ''), true);
  perform set_config('request.jwt.claim.role', coalesce(v_prev_role, ''), true);
  perform set_config('request.jwt.claims', coalesce(v_prev_claims, ''), true);

  if coalesce((v_submit_result->>'ok')::boolean, false) then
    update public.dataset_review_submit_requests
      set status = 'submitted',
          result = v_submit_result->'data',
          last_error_code = null,
          last_error_message = null,
          last_error_details = null,
          modified_at = now(),
          completed_at = now()
    where id = v_job.id
    returning *
      into v_job;

    insert into public.command_audit_log (
      command,
      actor_user_id,
      target_table,
      target_id,
      target_version,
      payload
    )
    values (
      'cmd_review_submit_from_job',
      v_job.requested_by,
      v_job.dataset_table,
      v_job.dataset_id,
      v_job.dataset_version,
      coalesce(p_audit, '{}'::jsonb) || jsonb_build_object(
        'review_submit_job_id', v_job.id,
        'review_submit_request_id', v_job.id,
        'gate_run_id', v_job.gate_run_id,
        'gate_worker_job_id', v_job.gate_worker_job_id
      )
    );

    return jsonb_build_object(
      'ok', true,
      'data', public.cmd_dataset_review_submit_job_payload(v_job)
    );
  end if;

  v_error_code := coalesce(v_submit_result->>'code', 'REVIEW_SUBMIT_JOB_ERROR');
  v_error_status := coalesce(nullif(v_submit_result->>'status', '')::integer, 500);
  v_error_message := coalesce(v_submit_result->>'message', 'Review-submit job failed');
  v_job_status := case
    when v_error_code = 'REVIEW_SUBMIT_GATE_NOT_READY' then 'waiting_gate'
    when v_error_code = 'REVIEW_SUBMIT_GATE_BLOCKED' then 'blocked'
    when v_error_code in ('REVIEW_SUBMIT_GATE_STALE', 'REVIEW_SUBMIT_JOB_STALE') then 'stale'
    else 'error'
  end;

  update public.dataset_review_submit_requests
    set status = v_job_status,
        last_error_code = case when v_job_status = 'waiting_gate' then null else v_error_code end,
        last_error_message = case when v_job_status = 'waiting_gate' then null else v_error_message end,
        last_error_details = case
          when v_job_status = 'waiting_gate' then null
          else jsonb_build_object('submitResult', v_submit_result)
        end,
        modified_at = now(),
        completed_at = case
          when v_job_status = 'waiting_gate' then null
          else now()
        end
  where id = v_job.id
  returning *
    into v_job;

  return jsonb_build_object(
    'ok', false,
    'code', v_error_code,
    'status', v_error_status,
    'message', v_error_message,
    'details', public.cmd_dataset_review_submit_job_payload(v_job)
  );
exception
  when others then
    perform set_config('request.jwt.claim.sub', coalesce(v_prev_sub, ''), true);
    perform set_config('request.jwt.claim.role', coalesce(v_prev_role, ''), true);
    perform set_config('request.jwt.claims', coalesce(v_prev_claims, ''), true);
    raise;
end;
$$;

drop view if exists public.worker_job_domain_refs;

create view public.worker_job_domain_refs
with (security_invoker = true)
as
select
  worker_job_id,
  'lca_results'::text as domain_source,
  id as domain_id,
  'lca_result_artifact'::text as domain_role,
  job_id as legacy_job_id,
  null::text as status,
  created_at,
  created_at as updated_at
from public.lca_results
where worker_job_id is not null
union all
select
  worker_job_id,
  'lca_result_cache'::text as domain_source,
  id as domain_id,
  'lca_result_cache'::text as domain_role,
  job_id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_result_cache
where worker_job_id is not null
union all
select
  worker_job_id,
  'lca_latest_all_unit_results'::text as domain_source,
  id as domain_id,
  'lca_latest_all_unit_result'::text as domain_role,
  job_id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_latest_all_unit_results
where worker_job_id is not null
union all
select
  prepared_worker_job_id as worker_job_id,
  'lca_factorization_registry'::text as domain_source,
  id as domain_id,
  'lca_factorization_artifact'::text as domain_role,
  prepared_job_id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_factorization_registry
where prepared_worker_job_id is not null
union all
select
  worker_job_id,
  'lca_package_artifacts'::text as domain_source,
  id as domain_id,
  'package_artifact'::text as domain_role,
  job_id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_package_artifacts
where worker_job_id is not null
union all
select
  worker_job_id,
  'lca_package_export_items'::text as domain_source,
  id as domain_id,
  'package_export_item'::text as domain_role,
  job_id as legacy_job_id,
  null::text as status,
  created_at,
  updated_at
from public.lca_package_export_items
where worker_job_id is not null
union all
select
  worker_job_id,
  'lca_package_request_cache'::text as domain_source,
  id as domain_id,
  'package_request_cache'::text as domain_role,
  job_id as legacy_job_id,
  status,
  created_at,
  updated_at
from public.lca_package_request_cache
where worker_job_id is not null
union all
select
  submit_worker_job_id as worker_job_id,
  'dataset_review_submit_requests'::text as domain_source,
  id as domain_id,
  'review_submit_coordinator'::text as domain_role,
  null::uuid as legacy_job_id,
  status,
  created_at,
  modified_at as updated_at
from public.dataset_review_submit_requests
where submit_worker_job_id is not null
union all
select
  worker_job_id,
  'dataset_review_submit_gate_runs'::text as domain_source,
  id as domain_id,
  'review_submit_gate_report'::text as domain_role,
  null::uuid as legacy_job_id,
  status,
  created_at,
  modified_at as updated_at
from public.dataset_review_submit_gate_runs
where worker_job_id is not null;

revoke all on public.worker_job_domain_refs from public, anon, authenticated;
grant select on public.worker_job_domain_refs to service_role;

comment on view public.worker_job_domain_refs
  is 'Service-role projection from canonical worker_jobs to retained non-legacy domain artifact/cache/history/coordinator rows. Legacy job tables are intentionally excluded so they can be retired with DROP RESTRICT after runtime cutover.';

create or replace view public.worker_legacy_table_retirement_blockers
with (security_invoker = true)
as
with legacy_targets as (
  select
    target_namespace.nspname as legacy_schema,
    target_class.relname as legacy_table,
    target_class.oid as table_oid,
    target_class.reltype as row_type_oid
  from (
    values
      ('public'::name, 'lca_jobs'::name),
      ('public'::name, 'lca_package_jobs'::name),
      ('public'::name, 'dataset_review_submit_jobs'::name)
  ) as targets(schema_name, table_name)
  join pg_namespace as target_namespace
    on target_namespace.nspname = targets.schema_name
  join pg_class as target_class
    on target_class.relnamespace = target_namespace.oid
   and target_class.relname = targets.table_name
   and target_class.relkind in ('r', 'p')
),
foreign_key_blockers as (
  select
    concat(legacy_schema, '.', legacy_table) as legacy_table,
    'foreign_key'::text as blocker_type,
    dependent_namespace.nspname::text as blocker_schema,
    dependent_class.relname::text as blocker_name,
    constraint_record.conname::text as blocker_identity,
    true as is_drop_restrict_blocker,
    jsonb_build_object(
      'constraintName', constraint_record.conname,
      'dependentTable', concat(dependent_namespace.nspname, '.', dependent_class.relname),
      'dependentColumns',
        (
          select jsonb_agg(dependent_attribute.attname order by dependent_attribute.attnum)
          from unnest(constraint_record.conkey) as constraint_column(attnum)
          join pg_attribute as dependent_attribute
            on dependent_attribute.attrelid = constraint_record.conrelid
           and dependent_attribute.attnum = constraint_column.attnum
        ),
      'referencedColumns',
        (
          select jsonb_agg(referenced_attribute.attname order by referenced_attribute.attnum)
          from unnest(constraint_record.confkey) as referenced_column(attnum)
          join pg_attribute as referenced_attribute
            on referenced_attribute.attrelid = constraint_record.confrelid
           and referenced_attribute.attnum = referenced_column.attnum
        ),
      'onDelete', constraint_record.confdeltype
    ) as details
  from legacy_targets
  join pg_constraint as constraint_record
    on constraint_record.confrelid = legacy_targets.table_oid
   and constraint_record.contype = 'f'
  join pg_class as dependent_class
    on dependent_class.oid = constraint_record.conrelid
  join pg_namespace as dependent_namespace
    on dependent_namespace.oid = dependent_class.relnamespace
  where constraint_record.conrelid <> legacy_targets.table_oid
),
view_blockers as (
  select distinct
    concat(legacy_schema, '.', legacy_table) as legacy_table,
    case dependent_class.relkind
      when 'm' then 'dependent_materialized_view'
      else 'dependent_view'
    end as blocker_type,
    dependent_namespace.nspname::text as blocker_schema,
    dependent_class.relname::text as blocker_name,
    concat(dependent_namespace.nspname, '.', dependent_class.relname)::text as blocker_identity,
    true as is_drop_restrict_blocker,
    jsonb_build_object(
      'dependentView', concat(dependent_namespace.nspname, '.', dependent_class.relname),
      'relkind', dependent_class.relkind
    ) as details
  from legacy_targets
  join pg_depend as dependency
    on dependency.refobjid = legacy_targets.table_oid
  join pg_rewrite as rewrite_rule
    on rewrite_rule.oid = dependency.objid
  join pg_class as dependent_class
    on dependent_class.oid = rewrite_rule.ev_class
  join pg_namespace as dependent_namespace
    on dependent_namespace.oid = dependent_class.relnamespace
  where dependent_class.oid <> legacy_targets.table_oid
    and dependent_class.relkind in ('v', 'm')
),
policy_blockers as (
  select distinct
    concat(legacy_schema, '.', legacy_table) as legacy_table,
    'policy'::text as blocker_type,
    dependent_namespace.nspname::text as blocker_schema,
    dependent_class.relname::text as blocker_name,
    concat(dependent_namespace.nspname, '.', dependent_class.relname, '.', policy_record.polname)::text as blocker_identity,
    true as is_drop_restrict_blocker,
    jsonb_build_object(
      'policyName', policy_record.polname,
      'dependentTable', concat(dependent_namespace.nspname, '.', dependent_class.relname),
      'command', policy_record.polcmd
    ) as details
  from legacy_targets
  join pg_depend as dependency
    on dependency.refobjid = legacy_targets.table_oid
  join pg_policy as policy_record
    on policy_record.oid = dependency.objid
  join pg_class as dependent_class
    on dependent_class.oid = policy_record.polrelid
  join pg_namespace as dependent_namespace
    on dependent_namespace.oid = dependent_class.relnamespace
  where dependent_class.oid <> legacy_targets.table_oid
),
function_signature_blockers as (
  select distinct
    concat(legacy_schema, '.', legacy_table) as legacy_table,
    'function_signature'::text as blocker_type,
    function_namespace.nspname::text as blocker_schema,
    function_record.proname::text as blocker_name,
    concat(
      function_namespace.nspname,
      '.',
      function_record.proname,
      '(',
      pg_get_function_identity_arguments(function_record.oid),
      ')'
    )::text as blocker_identity,
    true as is_drop_restrict_blocker,
    jsonb_build_object(
      'arguments', pg_get_function_arguments(function_record.oid),
      'result', pg_get_function_result(function_record.oid)
    ) as details
  from legacy_targets
  join (
    select *
    from pg_proc
    where prokind in ('f', 'p', 'w')
  ) as function_record
    on lower(pg_get_function_arguments(function_record.oid)) like '%' || lower(legacy_table) || '%'
    or lower(pg_get_function_result(function_record.oid)) like '%' || lower(legacy_table) || '%'
  join pg_namespace as function_namespace
    on function_namespace.oid = function_record.pronamespace
),
function_source_references as (
  select distinct
    concat(legacy_schema, '.', legacy_table) as legacy_table,
    'function_source_reference'::text as blocker_type,
    function_namespace.nspname::text as blocker_schema,
    function_record.proname::text as blocker_name,
    concat(
      function_namespace.nspname,
      '.',
      function_record.proname,
      '(',
      pg_get_function_identity_arguments(function_record.oid),
      ')'
    )::text as blocker_identity,
    false as is_drop_restrict_blocker,
    jsonb_build_object(
      'reason', 'Function body references the legacy table name; this may not block DROP TABLE RESTRICT, but it is a runtime migration blocker.',
      'arguments', pg_get_function_arguments(function_record.oid),
      'result', pg_get_function_result(function_record.oid)
    ) as details
  from legacy_targets
  join (
    select *
    from pg_proc
    where prokind in ('f', 'p', 'w')
  ) as function_record
    on lower(function_record.prosrc) like '%public.' || lower(legacy_table) || '%'
    or lower(function_record.prosrc) like '%from ' || lower(legacy_table) || '%'
    or lower(function_record.prosrc) like '%join ' || lower(legacy_table) || '%'
    or lower(function_record.prosrc) like '%update ' || lower(legacy_table) || '%'
    or lower(function_record.prosrc) like '%insert into ' || lower(legacy_table) || '%'
    or lower(function_record.prosrc) like '%delete from ' || lower(legacy_table) || '%'
    or lower(function_record.prosrc) like '%' || lower(legacy_table) || '%rowtype%'
  join pg_namespace as function_namespace
    on function_namespace.oid = function_record.pronamespace
  where function_namespace.nspname not in ('pg_catalog', 'information_schema')
)
select *
from foreign_key_blockers
union all
select *
from view_blockers
union all
select *
from policy_blockers
union all
select *
from function_signature_blockers
union all
select *
from function_source_references;

revoke all on public.worker_legacy_table_retirement_blockers from public, anon, authenticated;
grant select on public.worker_legacy_table_retirement_blockers to service_role;

comment on view public.worker_legacy_table_retirement_blockers
  is 'Service-role audit view for DROP TABLE RESTRICT blockers and real runtime function body references. Compatibility API names alone are not treated as table blockers.';

comment on table public.dataset_review_submit_requests
  is 'Durable review-submit request/coordinator state. This replaces dataset_review_submit_jobs as the active coordinator table while worker_jobs remains the canonical lifecycle fact.';

comment on column public.dataset_review_submit_requests.submit_worker_job_id
  is 'Canonical root review_submit.submit worker_jobs task for this review-submit request.';

comment on column public.dataset_review_submit_requests.gate_worker_job_id
  is 'Canonical review_submit.gate worker_jobs task that verifies this review-submit request before final submission.';

comment on table public.dataset_review_submit_jobs
  is 'Retired legacy review-submit job/coordinator table retained only for historical compatibility during cutover. Active review-submit coordinator state lives in public.dataset_review_submit_requests.';

comment on function util.process_dataset_review_submit_jobs(integer, integer, integer)
  is 'Invokes the Edge review-submit coordinator that advances retained review-submit request rows after worker gate results are available. The function name is kept for scheduling compatibility.';

revoke all on function public.cmd_dataset_review_submit_job_payload(anyelement) from public, anon, authenticated;
revoke all on function public.dataset_review_submit_requests_assign_submit_worker_job() from public, anon, authenticated;
revoke all on function public.dataset_review_submit_requests_sync_submit_worker_job() from public, anon, authenticated;
revoke all on function public.cmd_dataset_review_submit_job_enqueue(text, uuid, text, text, text, text, jsonb) from public, anon, authenticated;
revoke all on function public.cmd_dataset_review_submit_job_read(uuid) from public, anon, authenticated;
revoke all on function public.cmd_dataset_review_submit_job_read_latest(text, uuid, text, text) from public, anon, authenticated;
revoke all on function public.cmd_dataset_review_submit_job_claim(integer, integer) from public, anon, authenticated;
revoke all on function public.cmd_dataset_review_submit_job_record_result(uuid, text, uuid, jsonb, text, text, jsonb, jsonb) from public, anon, authenticated;
revoke all on function public.cmd_review_submit_from_job(uuid, jsonb) from public, anon, authenticated;

grant execute on function public.cmd_dataset_review_submit_job_payload(anyelement) to service_role;
grant execute on function public.cmd_dataset_review_submit_job_enqueue(text, uuid, text, text, text, text, jsonb) to authenticated, service_role;
grant execute on function public.cmd_dataset_review_submit_job_read(uuid) to authenticated, service_role;
grant execute on function public.cmd_dataset_review_submit_job_read_latest(text, uuid, text, text) to authenticated, service_role;
grant execute on function public.cmd_dataset_review_submit_job_claim(integer, integer) to service_role;
grant execute on function public.cmd_dataset_review_submit_job_record_result(uuid, text, uuid, jsonb, text, text, jsonb, jsonb) to service_role;
grant execute on function public.cmd_review_submit_from_job(uuid, jsonb) to service_role;

create table if not exists public.worker_job_kinds (
  job_kind text primary key,
  worker_runtime text not null default 'calculator',
  worker_queue text not null,
  default_visibility text not null default 'user',
  default_priority integer not null default 0,
  default_max_attempts integer not null default 3,
  default_lease_seconds integer not null default 300,
  payload_schema_version text not null,
  result_schema_version text,
  user_visible boolean not null default true,
  description text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint worker_job_kinds_runtime_check
    check (worker_runtime in ('calculator')),
  constraint worker_job_kinds_queue_check
    check (worker_queue in ('solver', 'review_submit_gate', 'package', 'maintenance')),
  constraint worker_job_kinds_visibility_check
    check (default_visibility in ('user', 'operator', 'system')),
  constraint worker_job_kinds_default_attempts_check
    check (default_max_attempts >= 0),
  constraint worker_job_kinds_default_lease_check
    check (default_lease_seconds between 1 and 86400)
);

alter table public.worker_job_kinds enable row level security;

revoke all on public.worker_job_kinds from anon, authenticated;
grant all on public.worker_job_kinds to service_role;

insert into public.worker_job_kinds (
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
  description
) values
  (
    'review_submit.gate',
    'calculator',
    'review_submit_gate',
    'user',
    100,
    3,
    900,
    'review_submit.gate.request.v1',
    'review_submit.gate.result.v1',
    true,
    'Review-submit numeric-stability verification gate'
  ),
  (
    'lca.solve_one',
    'calculator',
    'solver',
    'user',
    0,
    3,
    900,
    'lca.solve_one.request.v1',
    'lca.solve.result.v1',
    true,
    'Single demand LCA solve'
  ),
  (
    'lca.solve_batch',
    'calculator',
    'solver',
    'user',
    0,
    3,
    1800,
    'lca.solve_batch.request.v1',
    'lca.solve.result.v1',
    true,
    'Batch LCA solve'
  ),
  (
    'lca.solve_all_unit',
    'calculator',
    'solver',
    'user',
    0,
    3,
    3600,
    'lca.solve_all_unit.request.v1',
    'lca.solve.result.v1',
    true,
    'All-unit LCA solve'
  ),
  (
    'lca.build_snapshot',
    'calculator',
    'solver',
    'user',
    10,
    3,
    3600,
    'lca.build_snapshot.request.v1',
    'lca.snapshot.result.v1',
    true,
    'Build or rebuild an LCA network snapshot'
  ),
  (
    'lca.contribution_path',
    'calculator',
    'solver',
    'user',
    0,
    3,
    1800,
    'lca.contribution_path.request.v1',
    'lca.contribution_path.result.v1',
    true,
    'Contribution path analysis'
  ),
  (
    'lca.factorization_prepare',
    'calculator',
    'solver',
    'operator',
    0,
    2,
    3600,
    'lca.factorization_prepare.request.v1',
    'lca.factorization_prepare.result.v1',
    false,
    'Prepare or refresh calculator factorization artifacts'
  ),
  (
    'lca.snapshot_gc',
    'calculator',
    'maintenance',
    'operator',
    0,
    1,
    3600,
    'lca.snapshot_gc.request.v1',
    'lca.snapshot_gc.result.v1',
    false,
    'Snapshot artifact retention and garbage collection'
  ),
  (
    'lca.result_gc',
    'calculator',
    'maintenance',
    'operator',
    0,
    1,
    3600,
    'lca.result_gc.request.v1',
    'lca.result_gc.result.v1',
    false,
    'LCA result artifact and metadata retention'
  ),
  (
    'tidas.package_artifact_gc',
    'calculator',
    'maintenance',
    'operator',
    0,
    1,
    3600,
    'tidas.package_artifact_gc.request.v1',
    'tidas.package_artifact_gc.result.v1',
    false,
    'TIDAS package artifact retention'
  ),
  (
    'tidas.export_package',
    'calculator',
    'package',
    'user',
    0,
    3,
    1800,
    'tidas.export_package.request.v1',
    'tidas.export_package.result.v1',
    true,
    'TIDAS package export'
  ),
  (
    'tidas.import_package',
    'calculator',
    'package',
    'user',
    0,
    3,
    1800,
    'tidas.import_package.request.v1',
    'tidas.import_package.result.v1',
    true,
    'TIDAS package import'
  )
on conflict (job_kind) do update
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
    updated_at = now();

create table if not exists public.worker_jobs (
  id uuid primary key default gen_random_uuid(),

  job_kind text not null references public.worker_job_kinds(job_kind),
  worker_runtime text not null default 'calculator',
  worker_queue text not null,
  priority integer not null default 0,
  queue_key text,

  root_job_id uuid references public.worker_jobs(id),
  parent_job_id uuid references public.worker_jobs(id),

  subject_type text,
  subject_id uuid,
  subject_version text,
  requester_type text not null default 'user',
  requested_by uuid,
  team_id uuid,

  idempotency_key text,
  request_hash text,
  concurrency_key text,

  status text not null default 'queued',
  phase text,
  progress numeric,
  visibility text not null default 'user',
  run_after timestamptz not null default now(),

  attempt_count integer not null default 0,
  max_attempts integer not null default 3,
  leased_by text,
  lease_token uuid,
  lease_expires_at timestamptz,
  heartbeat_at timestamptz,
  timeout_at timestamptz,

  payload_schema_version text not null,
  payload_json jsonb not null default '{}'::jsonb,
  payload_ref jsonb,
  result_schema_version text,
  result_json jsonb,
  result_ref jsonb,
  diagnostics jsonb not null default '{}'::jsonb,

  error_code text,
  error_message text,
  error_details jsonb,
  blocker_codes text[] not null default '{}'::text[],
  resolution_scope text,
  retryable boolean,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  started_at timestamptz,
  finished_at timestamptz,
  expires_at timestamptz,
  cancelled_at timestamptz,
  cancelled_by uuid,

  constraint worker_jobs_status_check check (status in (
    'queued',
    'running',
    'waiting',
    'completed',
    'blocked',
    'stale',
    'failed',
    'cancelled'
  )),
  constraint worker_jobs_attempt_check check (
    attempt_count >= 0 and max_attempts >= 0 and attempt_count <= max_attempts
  ),
  constraint worker_jobs_runtime_check check (worker_runtime in ('calculator')),
  constraint worker_jobs_queue_check
    check (worker_queue in ('solver', 'review_submit_gate', 'package', 'maintenance')),
  constraint worker_jobs_visibility_check check (visibility in ('user', 'operator', 'system')),
  constraint worker_jobs_resolution_scope_check check (
    resolution_scope is null or resolution_scope in ('user', 'operator', 'system')
  ),
  constraint worker_jobs_requester_check check (
    (requester_type = 'user' and requested_by is not null)
    or requester_type in ('system', 'service', 'operator')
  ),
  constraint worker_jobs_progress_check check (
    progress is null or (progress >= 0 and progress <= 1)
  ),
  constraint worker_jobs_payload_object_check check (jsonb_typeof(payload_json) = 'object'),
  constraint worker_jobs_diagnostics_object_check check (jsonb_typeof(diagnostics) = 'object'),
  constraint worker_jobs_payload_ref_object_check check (
    payload_ref is null or jsonb_typeof(payload_ref) = 'object'
  ),
  constraint worker_jobs_result_object_check check (
    result_json is null or jsonb_typeof(result_json) = 'object'
  ),
  constraint worker_jobs_result_ref_object_check check (
    result_ref is null or jsonb_typeof(result_ref) = 'object'
  ),
  constraint worker_jobs_error_details_object_check check (
    error_details is null or jsonb_typeof(error_details) = 'object'
  ),
  constraint worker_jobs_blocked_explanation_check check (
    status <> 'blocked'
    or (cardinality(blocker_codes) > 0 and resolution_scope is not null)
  )
);

alter table public.worker_jobs enable row level security;

create index if not exists worker_jobs_claim_idx
  on public.worker_jobs (
    worker_runtime,
    worker_queue,
    priority desc,
    run_after asc,
    created_at asc
  )
  where status in ('queued', 'stale');

create index if not exists worker_jobs_expired_running_idx
  on public.worker_jobs (
    worker_runtime,
    worker_queue,
    lease_expires_at asc
  )
  where status = 'running';

create index if not exists worker_jobs_requested_by_updated_idx
  on public.worker_jobs (requested_by, updated_at desc)
  where visibility = 'user' and requested_by is not null;

create index if not exists worker_jobs_subject_updated_idx
  on public.worker_jobs (subject_type, subject_id, subject_version, updated_at desc)
  where subject_type is not null and subject_id is not null;

create index if not exists worker_jobs_parent_idx
  on public.worker_jobs (parent_job_id)
  where parent_job_id is not null;

create index if not exists worker_jobs_root_idx
  on public.worker_jobs (root_job_id)
  where root_job_id is not null;

create unique index if not exists worker_jobs_idempotency_active_uidx
  on public.worker_jobs (
    worker_runtime,
    job_kind,
    coalesce(requested_by, '00000000-0000-0000-0000-000000000000'::uuid),
    idempotency_key
  )
  where idempotency_key is not null
    and status in ('queued', 'running', 'waiting', 'stale', 'blocked');

create unique index if not exists worker_jobs_concurrency_active_uidx
  on public.worker_jobs (worker_runtime, worker_queue, concurrency_key)
  where concurrency_key is not null
    and status in ('queued', 'running', 'waiting', 'stale');

revoke all on public.worker_jobs from anon, authenticated;
grant all on public.worker_jobs to service_role;

create table if not exists public.worker_job_events (
  id uuid primary key default gen_random_uuid(),
  job_id uuid not null references public.worker_jobs(id) on delete cascade,
  event_type text not null,
  status text,
  phase text,
  progress numeric,
  worker_id text,
  lease_token uuid,
  message text,
  details jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint worker_job_events_status_check check (
    status is null or status in (
      'queued',
      'running',
      'waiting',
      'completed',
      'blocked',
      'stale',
      'failed',
      'cancelled'
    )
  ),
  constraint worker_job_events_progress_check check (
    progress is null or (progress >= 0 and progress <= 1)
  ),
  constraint worker_job_events_details_object_check check (jsonb_typeof(details) = 'object')
);

alter table public.worker_job_events enable row level security;

create index if not exists worker_job_events_job_created_idx
  on public.worker_job_events (job_id, created_at desc);

revoke all on public.worker_job_events from anon, authenticated;
grant all on public.worker_job_events to service_role;

create table if not exists public.worker_job_artifacts (
  id uuid primary key default gen_random_uuid(),
  job_id uuid not null references public.worker_jobs(id) on delete cascade,
  artifact_type text not null,
  storage_bucket text,
  storage_path text,
  content_type text,
  byte_size bigint,
  checksum_sha256 text,
  metadata jsonb not null default '{}'::jsonb,
  visibility text not null default 'operator',
  created_at timestamptz not null default now(),
  expires_at timestamptz,
  constraint worker_job_artifacts_byte_size_check check (
    byte_size is null or byte_size >= 0
  ),
  constraint worker_job_artifacts_checksum_check check (
    checksum_sha256 is null or checksum_sha256 ~ '^[a-f0-9]{64}$'
  ),
  constraint worker_job_artifacts_metadata_object_check check (jsonb_typeof(metadata) = 'object'),
  constraint worker_job_artifacts_visibility_check check (visibility in ('user', 'operator', 'system'))
);

alter table public.worker_job_artifacts enable row level security;

create index if not exists worker_job_artifacts_job_created_idx
  on public.worker_job_artifacts (job_id, created_at desc);

revoke all on public.worker_job_artifacts from anon, authenticated;
grant all on public.worker_job_artifacts to service_role;

create or replace function public.worker_job_payload(
  p_job public.worker_jobs,
  p_include_internal boolean default false
) returns jsonb
language sql
stable
set search_path = public, pg_temp
as $$
  select jsonb_strip_nulls(
    jsonb_build_object(
      'id', (p_job).id,
      'jobKind', (p_job).job_kind,
      'workerRuntime', (p_job).worker_runtime,
      'workerQueue', (p_job).worker_queue,
      'priority', (p_job).priority,
      'queueKey', (p_job).queue_key,
      'rootJobId', (p_job).root_job_id,
      'parentJobId', (p_job).parent_job_id,
      'subjectType', (p_job).subject_type,
      'subjectId', (p_job).subject_id,
      'subjectVersion', (p_job).subject_version,
      'requesterType', (p_job).requester_type,
      'requestedBy', (p_job).requested_by,
      'teamId', (p_job).team_id,
      'idempotencyKey', (p_job).idempotency_key,
      'requestHash', (p_job).request_hash,
      'concurrencyKey', (p_job).concurrency_key,
      'status', (p_job).status,
      'phase', (p_job).phase,
      'progress', (p_job).progress,
      'visibility', (p_job).visibility,
      'runAfter', to_jsonb((p_job).run_after),
      'attemptCount', (p_job).attempt_count,
      'maxAttempts', (p_job).max_attempts,
      'leasedBy', case when p_include_internal then (p_job).leased_by else null end,
      'leaseToken', case when p_include_internal then (p_job).lease_token else null end,
      'leaseExpiresAt', case when p_include_internal then to_jsonb((p_job).lease_expires_at) else null end,
      'heartbeatAt', to_jsonb((p_job).heartbeat_at),
      'timeoutAt', to_jsonb((p_job).timeout_at),
      'payloadSchemaVersion', (p_job).payload_schema_version,
      'payload', case when p_include_internal then (p_job).payload_json else null end,
      'payloadRef', case when p_include_internal then (p_job).payload_ref else null end,
      'resultSchemaVersion', (p_job).result_schema_version,
      'result', (p_job).result_json,
      'resultRef', case when p_include_internal then (p_job).result_ref else null end,
      'diagnostics', case when p_include_internal then (p_job).diagnostics else null end,
      'errorCode', (p_job).error_code,
      'errorMessage', (p_job).error_message,
      'errorDetails', case when p_include_internal then (p_job).error_details else null end,
      'blockerCodes', to_jsonb((p_job).blocker_codes),
      'resolutionScope', (p_job).resolution_scope,
      'retryable', (p_job).retryable,
      'createdAt', to_jsonb((p_job).created_at),
      'updatedAt', to_jsonb((p_job).updated_at),
      'startedAt', to_jsonb((p_job).started_at),
      'finishedAt', to_jsonb((p_job).finished_at),
      'expiresAt', to_jsonb((p_job).expires_at),
      'cancelledAt', to_jsonb((p_job).cancelled_at),
      'cancelledBy', (p_job).cancelled_by
    )
  )
$$;

create or replace function public.worker_enqueue_job(
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
set search_path = public, pg_temp
as $$
declare
  v_job_kind text := lower(trim(coalesce(p_job_kind, '')));
  v_requester_type text := lower(trim(coalesce(p_requester_type, 'user')));
  v_kind public.worker_job_kinds%rowtype;
  v_existing public.worker_jobs%rowtype;
  v_job public.worker_jobs%rowtype;
  v_payload jsonb := coalesce(p_payload_json, '{}'::jsonb);
  v_payload_ref jsonb := p_payload_ref;
  v_visibility text;
  v_payload_schema_version text;
  v_priority integer;
  v_max_attempts integer;
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
  from public.worker_job_kinds
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

  v_payload_schema_version := coalesce(nullif(trim(p_payload_schema_version), ''), v_kind.payload_schema_version);
  v_priority := coalesce(p_priority, v_kind.default_priority);
  v_max_attempts := greatest(1, coalesce(p_max_attempts, v_kind.default_max_attempts, 3));

  if p_idempotency_key is not null then
    select *
      into v_existing
    from public.worker_jobs
    where worker_runtime = v_kind.worker_runtime
      and job_kind = v_kind.job_kind
      and requested_by is not distinct from p_requested_by
      and idempotency_key = p_idempotency_key
      and status in ('queued', 'running', 'waiting', 'stale', 'blocked')
    order by created_at desc
    limit 1;

    if v_existing.id is not null then
      return jsonb_build_object(
        'ok', true,
        'data', public.worker_job_payload(v_existing, true),
        'reused', true
      );
    end if;
  end if;

  if p_concurrency_key is not null then
    select *
      into v_existing
    from public.worker_jobs
    where worker_runtime = v_kind.worker_runtime
      and worker_queue = v_kind.worker_queue
      and concurrency_key = p_concurrency_key
      and status in ('queued', 'running', 'waiting', 'stale')
    order by created_at desc
    limit 1;

    if v_existing.id is not null then
      return jsonb_build_object(
        'ok', false,
        'code', 'WORKER_JOB_CONCURRENCY_CONFLICT',
        'status', 409,
        'message', 'A conflicting worker job is already active',
        'details', public.worker_job_payload(v_existing, false)
      );
    end if;
  end if;

  insert into public.worker_jobs (
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
    nullif(trim(p_queue_key), ''),
    p_root_job_id,
    p_parent_job_id,
    nullif(trim(p_subject_type), ''),
    p_subject_id,
    nullif(trim(p_subject_version), ''),
    v_requester_type,
    p_requested_by,
    p_team_id,
    nullif(trim(p_idempotency_key), ''),
    nullif(trim(p_request_hash), ''),
    nullif(trim(p_concurrency_key), ''),
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

  insert into public.worker_job_events (
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
    'data', public.worker_job_payload(v_job, true),
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

create or replace function public.worker_claim_jobs(
  p_worker_queue text,
  p_worker_id text default null,
  p_limit integer default 10,
  p_lease_seconds integer default null
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
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

  if v_worker_queue not in ('solver', 'review_submit_gate', 'package', 'maintenance') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_QUEUE',
      'status', 400,
      'message', 'workerQueue must be solver, review_submit_gate, package, or maintenance'
    );
  end if;

  with expired as (
    update public.worker_jobs as j
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
    where j.worker_runtime = 'calculator'
      and j.worker_queue = v_worker_queue
      and j.status = 'running'
      and j.lease_expires_at < now()
      and j.attempt_count >= j.max_attempts
    returning j.*
  ),
  expired_events as (
    insert into public.worker_job_events (
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
    from public.worker_jobs
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
    update public.worker_jobs as j
      set status = 'running',
          attempt_count = j.attempt_count + 1,
          leased_by = v_worker_id,
          lease_token = gen_random_uuid(),
          lease_expires_at = now() + make_interval(secs => v_lease_seconds),
          heartbeat_at = now(),
          started_at = coalesce(j.started_at, now()),
          updated_at = now(),
          error_code = null,
          error_message = null,
          error_details = null
    from candidate
    where j.id = candidate.id
    returning j.*
  ),
  claim_events as (
    insert into public.worker_job_events (
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
  select coalesce(jsonb_agg(public.worker_job_payload(updated, true)), '[]'::jsonb)
    into v_jobs
  from updated;

  return jsonb_build_object(
    'ok', true,
    'data', v_jobs
  );
end;
$$;

create or replace function public.worker_heartbeat_job(
  p_job_id uuid,
  p_lease_token uuid,
  p_phase text default null,
  p_progress numeric default null,
  p_diagnostics jsonb default null,
  p_lease_seconds integer default 300
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_job public.worker_jobs%rowtype;
  v_lease_seconds integer := greatest(1, least(coalesce(p_lease_seconds, 300), 86400));
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
  from public.worker_jobs
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

  update public.worker_jobs
    set phase = coalesce(nullif(trim(p_phase), ''), phase),
        progress = coalesce(p_progress, progress),
        diagnostics = diagnostics || coalesce(p_diagnostics, '{}'::jsonb),
        heartbeat_at = now(),
        lease_expires_at = now() + make_interval(secs => v_lease_seconds),
        updated_at = now()
  where id = v_job.id
  returning *
    into v_job;

  insert into public.worker_job_events (
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
    jsonb_build_object(
      'leaseExpiresAt', v_job.lease_expires_at,
      'diagnostics', coalesce(p_diagnostics, '{}'::jsonb)
    )
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.worker_job_payload(v_job, true)
  );
end;
$$;

create or replace function public.worker_record_job_result(
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
set search_path = public, pg_temp
as $$
declare
  v_status text := lower(trim(coalesce(p_status, '')));
  v_resolution_scope text := lower(trim(coalesce(p_resolution_scope, '')));
  v_blocker_codes text[] := coalesce(p_blocker_codes, '{}'::text[]);
  v_job public.worker_jobs%rowtype;
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
  from public.worker_jobs
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

  update public.worker_jobs
    set status = v_status,
        result_schema_version = coalesce(nullif(trim(p_result_schema_version), ''), result_schema_version),
        result_json = p_result_json,
        result_ref = p_result_ref,
        diagnostics = diagnostics || coalesce(p_diagnostics, '{}'::jsonb),
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

  insert into public.worker_job_events (
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
    'data', public.worker_job_payload(v_job, true)
  );
end;
$$;

create or replace function public.worker_retry_job(
  p_job_id uuid,
  p_run_after timestamptz default null,
  p_max_attempts integer default null,
  p_reason text default null
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_job public.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to retry worker jobs'
    );
  end if;

  select *
    into v_job
  from public.worker_jobs
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

  if v_job.status not in ('failed', 'blocked', 'stale', 'waiting', 'cancelled') then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_RETRYABLE',
      'status', 409,
      'message', 'Worker job is not in a retryable status'
    );
  end if;

  update public.worker_jobs
    set status = 'queued',
        run_after = coalesce(p_run_after, now()),
        attempt_count = 0,
        max_attempts = greatest(1, coalesce(p_max_attempts, max_attempts)),
        leased_by = null,
        lease_token = null,
        lease_expires_at = null,
        error_code = null,
        error_message = null,
        error_details = null,
        blocker_codes = '{}'::text[],
        resolution_scope = null,
        retryable = null,
        cancelled_at = null,
        cancelled_by = null,
        finished_at = null,
        updated_at = now()
  where id = v_job.id
  returning *
    into v_job;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    message,
    details
  ) values (
    v_job.id,
    'retried',
    v_job.status,
    p_reason,
    jsonb_build_object('runAfter', v_job.run_after)
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.worker_job_payload(v_job, true)
  );
end;
$$;

create or replace function public.worker_cancel_job(
  p_job_id uuid,
  p_cancelled_by uuid default null,
  p_reason text default null
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_job public.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to cancel worker jobs'
    );
  end if;

  select *
    into v_job
  from public.worker_jobs
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

  if v_job.status in ('completed', 'blocked', 'failed') then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_TERMINAL',
      'status', 409,
      'message', 'Completed, blocked, and failed worker jobs cannot be cancelled'
    );
  end if;

  if v_job.status = 'cancelled' then
    return jsonb_build_object(
      'ok', true,
      'data', public.worker_job_payload(v_job, true)
    );
  end if;

  update public.worker_jobs
    set status = 'cancelled',
        cancelled_at = now(),
        cancelled_by = p_cancelled_by,
        leased_by = null,
        lease_token = null,
        lease_expires_at = null,
        error_code = 'worker_job_cancelled',
        error_message = coalesce(nullif(trim(p_reason), ''), 'Worker job was cancelled'),
        updated_at = now(),
        finished_at = now()
  where id = v_job.id
  returning *
    into v_job;

  insert into public.worker_job_events (
    job_id,
    event_type,
    status,
    message,
    details
  ) values (
    v_job.id,
    'cancelled',
    v_job.status,
    p_reason,
    jsonb_build_object('cancelledBy', p_cancelled_by)
  );

  return jsonb_build_object(
    'ok', true,
    'data', public.worker_job_payload(v_job, true)
  );
end;
$$;

create or replace function public.worker_read_job(
  p_job_id uuid,
  p_include_internal boolean default false
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_job public.worker_jobs%rowtype;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to read worker jobs'
    );
  end if;

  select *
    into v_job
  from public.worker_jobs
  where id = p_job_id;

  if v_job.id is null then
    return jsonb_build_object(
      'ok', false,
      'code', 'WORKER_JOB_NOT_FOUND',
      'status', 404,
      'message', 'Worker job not found'
    );
  end if;

  return jsonb_build_object(
    'ok', true,
    'data', public.worker_job_payload(v_job, p_include_internal)
  );
end;
$$;

create or replace function public.worker_list_jobs(
  p_requested_by uuid default null,
  p_subject_type text default null,
  p_subject_id uuid default null,
  p_statuses text[] default null,
  p_visibility text default null,
  p_limit integer default 50,
  p_include_internal boolean default false
) returns jsonb
language plpgsql
security definer
set search_path = public, pg_temp
as $$
declare
  v_limit integer := greatest(1, least(coalesce(p_limit, 50), 200));
  v_jobs jsonb := '[]'::jsonb;
begin
  if not coalesce(util.is_service_request(), false) then
    return jsonb_build_object(
      'ok', false,
      'code', 'SERVICE_ROLE_REQUIRED',
      'status', 403,
      'message', 'Service role is required to list worker jobs'
    );
  end if;

  if p_visibility is not null and p_visibility not in ('user', 'operator', 'system') then
    return jsonb_build_object(
      'ok', false,
      'code', 'INVALID_WORKER_JOB_VISIBILITY',
      'status', 400,
      'message', 'visibility must be user, operator, or system'
    );
  end if;

  select coalesce(jsonb_agg(public.worker_job_payload(j, p_include_internal) order by j.updated_at desc), '[]'::jsonb)
    into v_jobs
  from (
    select *
    from public.worker_jobs
    where (p_requested_by is null or requested_by = p_requested_by)
      and (p_subject_type is null or subject_type = p_subject_type)
      and (p_subject_id is null or subject_id = p_subject_id)
      and (p_visibility is null or visibility = p_visibility)
      and (p_statuses is null or status = any(p_statuses))
    order by updated_at desc
    limit v_limit
  ) as j;

  return jsonb_build_object(
    'ok', true,
    'data', v_jobs
  );
end;
$$;

revoke all on function public.worker_job_payload(public.worker_jobs, boolean) from public, anon, authenticated;
revoke all on function public.worker_enqueue_job(
  text,
  jsonb,
  text,
  text,
  uuid,
  text,
  uuid,
  text,
  uuid,
  text,
  text,
  text,
  integer,
  text,
  timestamptz,
  text,
  integer,
  timestamptz,
  jsonb,
  uuid,
  uuid
) from public, anon, authenticated;
revoke all on function public.worker_claim_jobs(text, text, integer, integer) from public, anon, authenticated;
revoke all on function public.worker_heartbeat_job(uuid, uuid, text, numeric, jsonb, integer) from public, anon, authenticated;
revoke all on function public.worker_record_job_result(
  uuid,
  uuid,
  text,
  jsonb,
  text,
  jsonb,
  jsonb,
  text,
  text,
  jsonb,
  text[],
  text,
  boolean
) from public, anon, authenticated;
revoke all on function public.worker_retry_job(uuid, timestamptz, integer, text) from public, anon, authenticated;
revoke all on function public.worker_cancel_job(uuid, uuid, text) from public, anon, authenticated;
revoke all on function public.worker_read_job(uuid, boolean) from public, anon, authenticated;
revoke all on function public.worker_list_jobs(uuid, text, uuid, text[], text, integer, boolean) from public, anon, authenticated;

grant execute on function public.worker_enqueue_job(
  text,
  jsonb,
  text,
  text,
  uuid,
  text,
  uuid,
  text,
  uuid,
  text,
  text,
  text,
  integer,
  text,
  timestamptz,
  text,
  integer,
  timestamptz,
  jsonb,
  uuid,
  uuid
) to service_role;
grant execute on function public.worker_claim_jobs(text, text, integer, integer) to service_role;
grant execute on function public.worker_heartbeat_job(uuid, uuid, text, numeric, jsonb, integer) to service_role;
grant execute on function public.worker_record_job_result(
  uuid,
  uuid,
  text,
  jsonb,
  text,
  jsonb,
  jsonb,
  text,
  text,
  jsonb,
  text[],
  text,
  boolean
) to service_role;
grant execute on function public.worker_retry_job(uuid, timestamptz, integer, text) to service_role;
grant execute on function public.worker_cancel_job(uuid, uuid, text) to service_role;
grant execute on function public.worker_read_job(uuid, boolean) to service_role;
grant execute on function public.worker_list_jobs(uuid, text, uuid, text[], text, integer, boolean) to service_role;
